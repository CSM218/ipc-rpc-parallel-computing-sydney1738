package pdc;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * Message represents the communication unit in the CSM218 protocol.
 * 
 * Custom binary wire format:
 * [magic:4][version:1][msgLen:4][messageTypeLen:1][messageType:*][studentIdLen:1][studentId:*]
 * [timestamp:8][payloadLen:4][payload:*]
 */
public class Message {
    public String magic;
    public int version;
    public String messageType;
    public String studentId;
    public long timestamp;
    public byte[] payload;

    public Message() {
        this.magic = "CSM218";
        this.version = 1;
        this.messageType = "";
        this.studentId = "";
        this.timestamp = System.currentTimeMillis();
        this.payload = new byte[0];
    }

    public Message(String messageType, String studentId, byte[] payload) {
        this.magic = "CSM218";
        this.version = 1;
        this.messageType = messageType;
        this.studentId = studentId;
        this.timestamp = System.currentTimeMillis();
        this.payload = payload != null ? payload : new byte[0];
    }

    /**
     * Converts the message to a byte stream for network transmission.
     * Uses length-prefixing for message framing in TCP streams.
     */
    public byte[] pack() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);

        // Write header with CSM218 magic
        dos.writeBytes("CSM218"); // 6 bytes for "CSM218"
        dos.writeByte(version); // 1 byte

        // Prepare content to calculate total message length
        ByteArrayOutputStream contentBaos = new ByteArrayOutputStream();
        DataOutputStream contentDos = new DataOutputStream(contentBaos);

        // Write messageType
        byte[] messageTypeBytes = messageType.getBytes(StandardCharsets.UTF_8);
        contentDos.writeByte(messageTypeBytes.length);
        contentDos.write(messageTypeBytes);

        // Write studentId
        byte[] studentIdBytes = studentId.getBytes(StandardCharsets.UTF_8);
        contentDos.writeByte(studentIdBytes.length);
        contentDos.write(studentIdBytes);

        // Write timestamp
        contentDos.writeLong(timestamp);

        // Write payload
        contentDos.writeInt(payload.length);
        contentDos.write(payload);

        byte[] content = contentBaos.toByteArray();

        // Write content length (includes everything after the 7-byte header)
        dos.writeInt(content.length);
        dos.write(content);

        return baos.toByteArray();
    }

    /**
     * Reconstructs a Message from a byte stream.
     */
    public static Message unpack(byte[] data) throws IOException {
        if (data == null || data.length < 7) {
            return null;
        }

        ByteArrayInputStream bais = new ByteArrayInputStream(data);
        DataInputStream dis = new DataInputStream(bais);

        // Read and validate magic
        byte[] magicBytes = new byte[6];
        dis.readFully(magicBytes);
        String magic = new String(magicBytes, StandardCharsets.UTF_8);
        if (!magic.equals("CSM218")) {
            throw new IOException("Invalid magic: " + magic);
        }

        // Read version
        int version = dis.readByte() & 0xFF;

        // Read message length
        int msgLen = dis.readInt();

        // Validate we have enough data
        if (bais.available() < msgLen) {
            return null;
        }

        Message msg = new Message();
        msg.magic = magic;
        msg.version = version;

        // Read messageType
        int messageTypeLen = dis.readByte() & 0xFF;
        byte[] messageTypeBytes = new byte[messageTypeLen];
        dis.readFully(messageTypeBytes);
        msg.messageType = new String(messageTypeBytes, StandardCharsets.UTF_8);

        // Read studentId
        int studentIdLen = dis.readByte() & 0xFF;
        byte[] studentIdBytes = new byte[studentIdLen];
        dis.readFully(studentIdBytes);
        msg.studentId = new String(studentIdBytes, StandardCharsets.UTF_8);

        // Read timestamp
        msg.timestamp = dis.readLong();

        // Read payload
        int payloadLen = dis.readInt();
        msg.payload = new byte[payloadLen];
        dis.readFully(msg.payload);

        return msg;
    }

    /**
     * Helper method to read a single message from a stream.
     * Handles length-prefixed message framing.
     */
    public static Message readFrom(DataInputStream dis) throws IOException {
        // Try to read the 7-byte header (6 for "CSM218" + 1 for version)
        byte[] headerBuffer = new byte[7];
        
        try {
            dis.readFully(headerBuffer);
        } catch (EOFException e) {
            return null;
        }

        // Validate magic
        String magic = new String(headerBuffer, 0, 6, StandardCharsets.UTF_8);
        if (!magic.equals("CSM218")) {
            throw new IOException("Invalid magic: " + magic);
        }

        // Read message length (version field not used in reconstruction)
        byte[] lenBuffer = new byte[4];
        dis.readFully(lenBuffer);
        int msgLen = ((lenBuffer[0] & 0xFF) << 24) |
                     ((lenBuffer[1] & 0xFF) << 16) |
                     ((lenBuffer[2] & 0xFF) << 8) |
                     (lenBuffer[3] & 0xFF);

        // Read the rest of the message
        byte[] content = new byte[msgLen];
        dis.readFully(content);

        // Now unpack the entire message
        byte[] fullMessage = new byte[11 + msgLen];
        System.arraycopy(headerBuffer, 0, fullMessage, 0, 7);
        System.arraycopy(lenBuffer, 0, fullMessage, 7, 4);
        System.arraycopy(content, 0, fullMessage, 11, msgLen);

        return unpack(fullMessage);
    }

    /**
     * Helper method to write a message to a data stream.
     */
    public void writeTo(DataOutputStream dos) throws IOException {
        byte[] packed = pack();
        dos.write(packed);
        dos.flush();
    }
}

class EOFException extends IOException {
}
