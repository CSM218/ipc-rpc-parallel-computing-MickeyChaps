package pdc;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

/**
 * Message represents the communication unit in the CSM218 protocol.
 *
 * Requirement: Implement a custom WIRE FORMAT.
 * DO NOT use JSON, XML, or standard Java Serialization.
 */
public class Message {
    public static final String MAGIC = "CSM218";
    public static final int VERSION = 1;

    public String magic;
    public int version;
    public String messageType;
    public String studentId;
    public long timestamp;
    public byte[] payload;

    public Message() {
        this.magic = MAGIC;
        this.version = VERSION;
        this.timestamp = System.currentTimeMillis();
    }

    public Message(String messageType, String studentId, byte[] payload) {
        this();
        this.messageType = messageType;
        this.studentId = studentId;
        this.payload = payload;
    }

    public void validate() {
        if (!MAGIC.equals(magic)) {
            throw new IllegalArgumentException("Invalid magic: " + magic);
        }
        if (version != VERSION) {
            throw new IllegalArgumentException("Unsupported version: " + version);
        }
        if (messageType == null || messageType.isEmpty()) {
            throw new IllegalArgumentException("Missing messageType");
        }
        if (studentId == null || studentId.isEmpty()) {
            throw new IllegalArgumentException("Missing studentId");
        }
        if (payload == null) {
            payload = new byte[0];
        }
    }

    /**
     * Converts the message to a byte stream for network transmission.
     * Frame format: [int bodyLength][body...]
     */
    public byte[] pack() {
        validate();
        try {
            ByteArrayOutputStream body = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(body);
            writeString(out, magic);
            out.writeInt(version);
            writeString(out, messageType);
            writeString(out, studentId);
            out.writeLong(timestamp);
            out.writeInt(payload.length);
            out.write(payload);
            out.flush();

            byte[] bodyBytes = body.toByteArray();
            ByteArrayOutputStream frame = new ByteArrayOutputStream();
            DataOutputStream framedOut = new DataOutputStream(frame);
            framedOut.writeInt(bodyBytes.length);
            framedOut.write(bodyBytes);
            framedOut.flush();
            return frame.toByteArray();
        } catch (IOException e) {
            throw new IllegalStateException("Failed to pack message", e);
        }
    }

    /**
     * Reconstructs a Message from a byte stream.
     */
    public static Message unpack(byte[] data) {
        try {
            ByteArrayInputStream raw = new ByteArrayInputStream(data);
            DataInputStream in = new DataInputStream(raw);

            int bodyLength = in.readInt();
            if (bodyLength < 0 || bodyLength > data.length - Integer.BYTES) {
                bodyLength = data.length;
                raw.reset();
                in = new DataInputStream(raw);
            }

            byte[] body = new byte[bodyLength];
            in.readFully(body);

            DataInputStream bodyIn = new DataInputStream(new ByteArrayInputStream(body));
            Message msg = new Message();
            msg.magic = readString(bodyIn);
            msg.version = bodyIn.readInt();
            msg.messageType = readString(bodyIn);
            msg.studentId = readString(bodyIn);
            msg.timestamp = bodyIn.readLong();
            int payloadLength = bodyIn.readInt();
            if (payloadLength < 0) {
                payloadLength = 0;
            }
            msg.payload = new byte[payloadLength];
            if (payloadLength > 0) {
                bodyIn.readFully(msg.payload);
            }
            msg.validate();
            return msg;
        } catch (IOException e) {
            throw new IllegalArgumentException("Failed to unpack message", e);
        }
    }

    public void writeTo(OutputStream outputStream) throws IOException {
        byte[] frame = pack();
        outputStream.write(frame);
        outputStream.flush();
    }

    public static Message readFrom(InputStream inputStream) throws IOException {
        DataInputStream in = new DataInputStream(inputStream);
        int bodyLength = in.readInt();
        if (bodyLength <= 0) {
            throw new IOException("Invalid frame length");
        }
        byte[] body = new byte[bodyLength];
        in.readFully(body);
        return unpack(withLengthPrefix(body));
    }

    private static byte[] withLengthPrefix(byte[] body) throws IOException {
        ByteArrayOutputStream frame = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(frame);
        out.writeInt(body.length);
        out.write(body);
        out.flush();
        return frame.toByteArray();
    }

    static void writeString(DataOutputStream out, String value) throws IOException {
        if (value == null) {
            out.writeInt(-1);
            return;
        }
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        out.writeInt(bytes.length);
        out.write(bytes);
    }

    static String readString(DataInputStream in) throws IOException {
        int length = in.readInt();
        if (length < 0) {
            return null;
        }
        byte[] bytes = new byte[length];
        in.readFully(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    static void writeMatrix(DataOutputStream out, int[][] matrix) throws IOException {
        if (matrix == null) {
            out.writeInt(0);
            out.writeInt(0);
            return;
        }
        int rows = matrix.length;
        int cols = rows == 0 ? 0 : matrix[0].length;
        out.writeInt(rows);
        out.writeInt(cols);
        for (int i = 0; i < rows; i++) {
            for (int j = 0; j < cols; j++) {
                out.writeInt(matrix[i][j]);
            }
        }
    }

    static int[][] readMatrix(DataInputStream in) throws IOException {
        int rows = in.readInt();
        int cols = in.readInt();
        if (rows <= 0 || cols <= 0) {
            return new int[0][0];
        }
        int[][] matrix = new int[rows][cols];
        for (int i = 0; i < rows; i++) {
            for (int j = 0; j < cols; j++) {
                matrix[i][j] = in.readInt();
            }
        }
        return matrix;
    }
}
