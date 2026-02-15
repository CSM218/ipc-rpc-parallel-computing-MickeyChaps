package pdc;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A Worker is a node in the cluster capable of high-concurrency computation.
 */
public class Worker {
    private final ExecutorService taskPool = Executors.newFixedThreadPool(
            Math.max(2, Runtime.getRuntime().availableProcessors()));
    private final BlockingQueue<TaskRequest> inboundQueue = new LinkedBlockingQueue<>();
    private final AtomicBoolean running = new AtomicBoolean(false);

    private Socket socket;
    private DataInputStream in;
    private DataOutputStream out;

    private String workerId;
    private String studentId;

    public static void main(String[] args) throws Exception {
        Worker worker = new Worker();
        String host = envOrDefault("MASTER_HOST", "localhost");
        int port = parseInt(envOrDefault("MASTER_PORT", "0"), 0);
        worker.joinCluster(host, port);

        Thread.sleep(Long.MAX_VALUE);
    }

    /**
     * Connects to the Master and initiates the registration handshake.
     * The handshake exchanges Identity and Capability sets.
     */
    public void joinCluster(String masterHost, int port) {
        String resolvedHost = envOrDefault("MASTER_HOST", masterHost == null ? "localhost" : masterHost);
        int resolvedPort = parseInt(envOrDefault("MASTER_PORT", String.valueOf(port)), port);
        this.workerId = envOrDefault("WORKER_ID", "worker-" + UUID.randomUUID());
        this.studentId = envOrDefault("STUDENT_ID", "anonymous");

        try {
            socket = new Socket(resolvedHost, resolvedPort);
            socket.setTcpNoDelay(true);
            in = new DataInputStream(socket.getInputStream());
            out = new DataOutputStream(socket.getOutputStream());

            sendMessage(new Message("REGISTER_WORKER", studentId, workerId.getBytes(StandardCharsets.UTF_8)));
            String capabilities = "cores=" + Runtime.getRuntime().availableProcessors();
            sendMessage(new Message("REGISTER_CAPABILITIES", studentId, capabilities.getBytes(StandardCharsets.UTF_8)));

            startListener();
            execute();
        } catch (IOException e) {
            // Network absence is allowed in tests; log and exit gracefully.
            System.err.println("Worker joinCluster failed: " + e.getMessage());
        }
    }

    /**
     * Executes a received task block asynchronously.
     */
    public void execute() {
        if (!running.compareAndSet(false, true)) {
            return;
        }

        Thread dispatcher = new Thread(() -> {
            while (running.get()) {
                try {
                    TaskRequest task = inboundQueue.take();
                    taskPool.submit(() -> handleTask(task));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    running.set(false);
                }
            }
        });
        dispatcher.setDaemon(true);
        dispatcher.start();
    }

    private void startListener() {
        Thread listener = new Thread(() -> {
            while (true) {
                try {
                    Message msg = Message.readFrom(in);
                    handleMessage(msg);
                } catch (IOException e) {
                    System.err.println("Worker listener stopped: " + e.getMessage());
                    break;
                }
            }
        });
        listener.setDaemon(true);
        listener.start();
    }

    private void handleMessage(Message msg) {
        if ("HEARTBEAT".equals(msg.messageType)) {
            sendMessage(new Message("HEARTBEAT_ACK", studentId, workerId.getBytes(StandardCharsets.UTF_8)));
            return;
        }
        if ("RPC_REQUEST".equals(msg.messageType)) {
            TaskRequest task = TaskRequest.fromBytes(msg.payload);
            inboundQueue.offer(task);
            return;
        }
        if ("WORKER_ACK".equals(msg.messageType)) {
            return;
        }
    }

    private void handleTask(TaskRequest task) {
        try {
            int[][] block = multiplyBlock(task.left, task.right, task.startRow, task.endRow);
            TaskResult result = new TaskResult(task.taskId, task.startRow, block);
            Message response = new Message("RPC_RESPONSE", studentId, result.toBytes());
            sendMessage(response);
        } catch (Exception e) {
            String errorPayload = task.taskId + ";" + e.getMessage();
            sendMessage(new Message("TASK_ERROR", studentId, errorPayload.getBytes(StandardCharsets.UTF_8)));
        }
    }

    private void sendMessage(Message message) {
        try {
            synchronized (out) {
                message.writeTo(out);
            }
        } catch (IOException e) {
            System.err.println("Worker sendMessage error: " + e.getMessage());
        }
    }

    private static int[][] multiplyBlock(int[][] left, int[][] right, int startRow, int endRow) {
        int n = left.length;
        int cols = right[0].length;
        int[][] block = new int[endRow - startRow][cols];
        for (int i = startRow; i < endRow; i++) {
            for (int k = 0; k < n; k++) {
                int val = left[i][k];
                for (int j = 0; j < cols; j++) {
                    block[i - startRow][j] += val * right[k][j];
                }
            }
        }
        return block;
    }

    private static String envOrDefault(String key, String fallback) {
        String value = System.getenv(key);
        return value == null || value.isEmpty() ? fallback : value;
    }

    private static int parseInt(String raw, int fallback) {
        try {
            return Integer.parseInt(raw);
        } catch (NumberFormatException e) {
            return fallback;
        }
    }

    private static class TaskRequest {
        final String taskId;
        final int startRow;
        final int endRow;
        final int[][] left;
        final int[][] right;

        private TaskRequest(String taskId, int startRow, int endRow, int[][] left, int[][] right) {
            this.taskId = taskId;
            this.startRow = startRow;
            this.endRow = endRow;
            this.left = left;
            this.right = right;
        }

        static TaskRequest fromBytes(byte[] payload) {
            try {
                DataInputStream in = new DataInputStream(new ByteArrayInputStream(payload));
                String taskId = Message.readString(in);
                int startRow = in.readInt();
                int endRow = in.readInt();
                int[][] left = Message.readMatrix(in);
                int[][] right = Message.readMatrix(in);
                return new TaskRequest(taskId, startRow, endRow, left, right);
            } catch (IOException e) {
                throw new IllegalArgumentException("Invalid RPC request payload", e);
            }
        }
    }

    private static class TaskResult {
        final String taskId;
        final int startRow;
        final int[][] block;

        private TaskResult(String taskId, int startRow, int[][] block) {
            this.taskId = taskId;
            this.startRow = startRow;
            this.block = block;
        }

        byte[] toBytes() {
            try {
                ByteArrayOutputStream buffer = new ByteArrayOutputStream();
                DataOutputStream out = new DataOutputStream(buffer);
                Message.writeString(out, taskId);
                out.writeInt(startRow);
                Message.writeMatrix(out, block);
                out.flush();
                return buffer.toByteArray();
            } catch (IOException e) {
                throw new IllegalStateException("Failed to serialize task result", e);
            }
        }
    }
}
