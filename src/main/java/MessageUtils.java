import java.io.InputStream;
import java.io.OutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;


public class MessageUtils {

    public static int readMessageLength(InputStream reader) throws IOException {
        byte[] lenWord = new byte[4];
        if (reader.read(lenWord) == 4) {
            ByteBuffer buffer = wrap(lenWord);
            return buffer.getInt();
        } else {
            System.err.println("Message length not available");
            return -1;
        }
    }

    public static byte[] readMessage(InputStream reader, int messageLength) throws IOException {
        byte[] message = new byte[messageLength];
        if (reader.read(message) != messageLength) {
            System.err.println("Message length does not match");
            return null;
        }
        return message;
    }

    public static void processMessage(OutputStream writer, byte[] message) throws IOException {
        ByteBuffer buffer = wrap(message);
        //RequestKey key = RequestKey.fromValue(buffer.getShort());
	//short key = (short)APIKeys.RespondAPIKeyRequest(buffer.getShort());
	APIKeys key = APIKeys.fromApiKey(buffer.getShort());
        int version = buffer.getShort();
        int correlationId = buffer.getInt();
        System.out.println("Received request for " + key + " " + version + " " + correlationId);

        ByteBuffer responseBuffer = null;
        switch (key) {
            case APIKeys.API_VERSIONS:
                responseBuffer = handleApiVersions(version, correlationId, key);
                break;
            // Handle other cases (PRODUCE, FETCH, HEARTBEAT)
        }

        if (responseBuffer != null) {
            writer.write(data(responseBuffer));
            writer.flush();
        }
    }

 public  static ByteBuffer handleApiVersions(int version, int correlationId, APIKeys key) {
        ByteBuffer message = createApiVersionsResponse(version, correlationId, key);
        return createResponseBuffer(message);
    }

    public static ByteBuffer createApiVersionsResponse(int version, int correlationId, APIKeys key) {
        ByteBuffer message = ByteBuffer.allocate(1024).order(ByteOrder.BIG_ENDIAN);
        message.putInt(correlationId);

	// Error code 0 (no error)
        if (version >= 0 && version <= 4) {
            message.putShort((short) 0); // No error
            message.put((byte) 2) // compat arrays
				  

            // APIVersions
            .putShort((short)key.getCode()) // 1st KEY
	    .putShort((short) 3) // Min version
            .putShort((short) 4) // Max version
	
            // DescribeTopicPartitions
            .putShort((short)key.getCode()) // 2nd KEY
            .putShort((short) 0) // Min Version
            .putShort((short) 0) // Max Version
	
            // END
            .put((byte) 0) // TAG BUFFER
            .putInt(0) // throttle_time_ms
            .put((byte) 0); // TAG BUFFER
	    
        } else {
            message.putShort((short) 35); // Unsupported version error code
        }
        return message;
    }

    public static ByteBuffer createResponseBuffer(ByteBuffer message) {
        ByteBuffer response = ByteBuffer.allocate(1024).order(ByteOrder.BIG_ENDIAN);
        byte[] messageBytes = data(message);
        response.putInt(messageBytes.length);
        response.put(messageBytes);
        return response;
    }

    public static byte[] data(ByteBuffer buffer) {
        buffer.flip();
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        return bytes;
    }

    public static ByteBuffer wrap(byte[] data) {
        ByteBuffer buffer = ByteBuffer.wrap(data).order(ByteOrder.BIG_ENDIAN);
        return buffer;
    }
}



