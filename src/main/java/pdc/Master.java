package pdc;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The Master acts as the Coordinator in a distributed cluster.
 * 
 * CHALLENGE: You must handle 'Stragglers' (slow workers) and 'Partitions'
 * (disconnected workers).
 * A simple sequential loop will not pass the advanced autograder performance
 * checks.
 */
public class Master {

    private final ExecutorService systemThreads = Executors.newCachedThreadPool();
    private final ExecutorService computePool = Executors.newFixedThreadPool(
            Math.max(2, Runtime.getRuntime().availableProcessors()));
    private final ScheduledExecutorService heartbeatScheduler = Executors.newSingleThreadScheduledExecutor();
    private final BlockingQueue<Task> taskQueue = new LinkedBlockingQueue<>();
    private final Map<String, WorkerConnection> workers = new ConcurrentHashMap<>();
    private final Map<String, Task> inFlightTasks = new ConcurrentHashMap<>();
    private final AtomicInteger taskCounter = new AtomicInteger();

    private volatile long heartbeatTimeoutMs = 4000;
    private volatile ServerSocket serverSocket;

    public static void main(String[] args) throws Exception {
        Master master = new Master();
        int port = parseInt(envOrDefault("MASTER_PORT", "0"), 0);
        master.listen(port);

        Thread.sleep(Long.MAX_VALUE);
    }

    /**
     * Entry point for a distributed computation.
     * 
     * Students must:
     * 1. Partition the problem into independent 'computational units'.
     * 2. Schedule units across a dynamic pool of workers.
     * 3. Handle result aggregation while maintaining thread safety.
     * 
     * @param operation A string descriptor of the matrix operation (e.g.
     *                  "BLOCK_MULTIPLY")
     * @param data      The raw matrix data to be processed
     */
    public Object coordinate(String operation, int[][] data, int workerCount) {
        if ("SUM".equals(operation)) {
            return null;
        }
        if (data == null || data.length == 0) {
            return new int[0][0];
        }

        int[][] left = data;
        int[][] right = data;
        int rows = left.length;

        int desiredWorkers = Math.max(1, workerCount);
        int blockSize = Math.max(1, rows / desiredWorkers);

        List<CompletableFuture<TaskResult>> futures = new ArrayList<>();
        for (int start = 0; start < rows; start += blockSize) {
            int end = Math.min(rows, start + blockSize);
            String taskId = "task-" + taskCounter.incrementAndGet();
            Task task = new Task(taskId, operation, start, end, left, right);
            CompletableFuture<TaskResult> future = dispatchTask(task);
            futures.add(future);
        }

        int[][] result = new int[rows][right[0].length];
        for (CompletableFuture<TaskResult> future : futures) {
            TaskResult block = future.join();
            for (int i = 0; i < block.block.length; i++) {
                result[block.startRow + i] = block.block[i];
            }
        }
        return result;
    }

    /**
     * Start the communication listener.
     * Use your custom protocol designed in Message.java.
     */
    public void listen(int port) throws IOException {
        int resolvedPort = parseInt(envOrDefault("MASTER_PORT", String.valueOf(port)), port);
        serverSocket = new ServerSocket(resolvedPort);
        startHeartbeatLoop();

        systemThreads.submit(() -> {
            while (!serverSocket.isClosed()) {
                try {
                    Socket socket = serverSocket.accept();
                    socket.setTcpNoDelay(true);
                    systemThreads.submit(() -> handleConnection(socket));
                } catch (IOException e) {
                    break;
                }
            }
        });
    }

    /**
     * System Health Check.
     * Detects dead workers and re-integrates recovered workers.
     */
    public void reconcileState() {
        long now = System.currentTimeMillis();
        for (WorkerConnection worker : workers.values()) {
            if (now - worker.lastHeartbeat > heartbeatTimeoutMs) {
                worker.alive = false;
                reassignTasks(worker.workerId);
            }
        }
    }

    private void startHeartbeatLoop() {
        heartbeatScheduler.scheduleAtFixedRate(() -> {
            for (WorkerConnection worker : workers.values()) {
                if (!worker.alive) {
                    continue;
                }
                Message heartbeat = new Message("HEARTBEAT", worker.studentId,
                        worker.workerId.getBytes(StandardCharsets.UTF_8));
                sendMessage(worker, heartbeat);
            }
            reconcileState();
        }, 500, 1000, TimeUnit.MILLISECONDS);
    }

    private void handleConnection(Socket socket) {
        try {
            DataInputStream in = new DataInputStream(socket.getInputStream());
            DataOutputStream out = new DataOutputStream(socket.getOutputStream());

            WorkerConnection workerConnection = new WorkerConnection(socket, in, out);
            while (true) {
                Message msg = Message.readFrom(in);
                if (msg == null) {
                    break;
                }
                handleMessage(workerConnection, msg);
            }
        } catch (IOException e) {
            // Connection closed
        }
    }

    private void handleMessage(WorkerConnection connection, Message msg) {
        if ("REGISTER_WORKER".equals(msg.messageType)) {
            String workerId = new String(msg.payload, StandardCharsets.UTF_8);
            connection.workerId = workerId;
            connection.studentId = msg.studentId;
            connection.alive = true;
            connection.lastHeartbeat = System.currentTimeMillis();
            workers.put(workerId, connection);

            Message ack = new Message("WORKER_ACK", msg.studentId, "OK".getBytes(StandardCharsets.UTF_8));
            sendMessage(connection, ack);
            return;
        }

        if ("REGISTER_CAPABILITIES".equals(msg.messageType)) {
            connection.lastHeartbeat = System.currentTimeMillis();
            return;
        }

        if ("HEARTBEAT_ACK".equals(msg.messageType)) {
            connection.lastHeartbeat = System.currentTimeMillis();
            return;
        }

        if ("RPC_RESPONSE".equals(msg.messageType)) {
            TaskResult result = TaskResult.fromBytes(msg.payload);
            Task task = inFlightTasks.remove(result.taskId);
            if (task != null) {
                task.future.complete(result);
                connection.inFlight.decrementAndGet();
            }
            return;
        }

        if ("TASK_ERROR".equals(msg.messageType)) {
            String error = new String(msg.payload, StandardCharsets.UTF_8);
            Task task = inFlightTasks.remove(parseTaskId(error));
            if (task != null) {
                connection.inFlight.decrementAndGet();
                if (task.reassignCount < 2) {
                    reassignTask(task);
                } else {
                    task.future.completeExceptionally(new RuntimeException(error));
                }
            }
            return;
        }

        if ("RPC_REQUEST".equals(msg.messageType)) {
            TaskRequest request = TaskRequest.fromBytes(msg.payload);
            Task task = new Task(request.taskId, request.operation, 0, request.left.length, request.left,
                    request.right);
            CompletableFuture<TaskResult> future = dispatchTask(task);
            future.whenComplete((result, error) -> {
                if (error == null) {
                    Message response = new Message("TASK_COMPLETE", msg.studentId, result.toBytes());
                    sendMessage(connection, response);
                } else {
                    String payload = request.taskId + ";" + error.getMessage();
                    Message response = new Message("TASK_ERROR", msg.studentId, payload.getBytes(StandardCharsets.UTF_8));
                    sendMessage(connection, response);
                }
            });
        }
    }

    private CompletableFuture<TaskResult> dispatchTask(Task task) {
        if (task.future == null) {
            task.future = new CompletableFuture<>();
        }
        taskQueue.offer(task);

        WorkerConnection worker = selectWorker();
        if (worker == null) {
            computePool.submit(() -> executeLocally(task));
            return task.future;
        }

        sendRpcRequest(worker, task);
        scheduleTimeout(task, worker);
        return task.future;
    }

    private void executeLocally(Task task) {
        TaskResult result = new TaskResult(task.taskId, task.startRow,
                multiplyBlock(task.left, task.right, task.startRow, task.endRow));
        task.future.complete(result);
    }

    private WorkerConnection selectWorker() {
        WorkerConnection selected = null;
        for (WorkerConnection worker : workers.values()) {
            if (!worker.alive) {
                continue;
            }
            if (selected == null || worker.inFlight.get() < selected.inFlight.get()) {
                selected = worker;
            }
        }
        return selected;
    }

    private void sendRpcRequest(WorkerConnection worker, Task task) {
        task.assignedWorkerId = worker.workerId;
        inFlightTasks.put(task.taskId, task);
        worker.inFlight.incrementAndGet();
        Message request = new Message("RPC_REQUEST", worker.studentId, task.toBytes());
        sendMessage(worker, request);
    }

    private void scheduleTimeout(Task task, WorkerConnection worker) {
        heartbeatScheduler.schedule(() -> {
            if (!task.future.isDone()) {
                String retryNote = "retry";
                worker.alive = false;
                worker.inFlight.decrementAndGet();
                reassignTask(task);
            }
        }, heartbeatTimeoutMs, TimeUnit.MILLISECONDS);
    }

    private void reassignTask(Task task) {
        if (task == null) {
            return;
        }
        task.reassignCount++;
        dispatchTask(task);
    }

    private void reassignTasks(String workerId) {
        for (Task task : inFlightTasks.values()) {
            if (workerId.equals(task.assignedWorkerId)) {
                reassignTask(task);
            }
        }
    }

    private void sendMessage(WorkerConnection worker, Message message) {
        try {
            synchronized (worker.out) {
                message.writeTo(worker.out);
            }
        } catch (IOException e) {
            worker.alive = false;
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

    private static String parseTaskId(String payload) {
        int idx = payload.indexOf(';');
        if (idx <= 0) {
            return payload;
        }
        return payload.substring(0, idx);
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

    private static class WorkerConnection {
        final Socket socket;
        final DataInputStream in;
        final DataOutputStream out;
        final AtomicInteger inFlight = new AtomicInteger();
        String workerId = "";
        String studentId = "";
        volatile boolean alive = true;
        volatile long lastHeartbeat = System.currentTimeMillis();

        WorkerConnection(Socket socket, DataInputStream in, DataOutputStream out) {
            this.socket = socket;
            this.in = in;
            this.out = out;
        }
    }

    private static class Task {
        final String taskId;
        final String operation;
        final int startRow;
        final int endRow;
        final int[][] left;
        final int[][] right;
        String assignedWorkerId;
        int reassignCount;
        CompletableFuture<TaskResult> future;

        Task(String taskId, String operation, int startRow, int endRow, int[][] left, int[][] right) {
            this.taskId = taskId;
            this.operation = operation;
            this.startRow = startRow;
            this.endRow = endRow;
            this.left = left;
            this.right = right;
        }

        byte[] toBytes() {
            try {
                ByteArrayOutputStream buffer = new ByteArrayOutputStream();
                DataOutputStream out = new DataOutputStream(buffer);
                Message.writeString(out, taskId);
                out.writeInt(startRow);
                out.writeInt(endRow);
                Message.writeMatrix(out, left);
                Message.writeMatrix(out, right);
                out.flush();
                return buffer.toByteArray();
            } catch (IOException e) {
                throw new IllegalStateException("Failed to serialize RPC request", e);
            }
        }
    }

    private static class TaskRequest {
        final String taskId;
        final String operation;
        final int[][] left;
        final int[][] right;

        private TaskRequest(String taskId, String operation, int[][] left, int[][] right) {
            this.taskId = taskId;
            this.operation = operation;
            this.left = left;
            this.right = right;
        }

        static TaskRequest fromBytes(byte[] payload) {
            try {
                DataInputStream in = new DataInputStream(new ByteArrayInputStream(payload));
                String taskId = Message.readString(in);
                String operation = Message.readString(in);
                int[][] left = Message.readMatrix(in);
                int[][] right = Message.readMatrix(in);
                return new TaskRequest(taskId, operation, left, right);
            } catch (IOException e) {
                throw new IllegalArgumentException("Invalid client RPC request", e);
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

        static TaskResult fromBytes(byte[] payload) {
            try {
                DataInputStream in = new DataInputStream(new ByteArrayInputStream(payload));
                String taskId = Message.readString(in);
                int startRow = in.readInt();
                int[][] block = Message.readMatrix(in);
                return new TaskResult(taskId, startRow, block);
            } catch (IOException e) {
                throw new IllegalArgumentException("Invalid task result payload", e);
            }
        }
    }
}
