import java.io.InputStream;
import java.io.OutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

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
	    case APIKeys.DESCRIBE_TOPIC_PARTITIONS:
		String topicName = extractTopicName(buffer);
		UUID topicUUID = UUID.fromString("00000000-0000-0000-0000-000000000000");
		//responseBuffer = handleTopicPartitionsRequest(correlationId, topicName, topicUUID);
		responseBuffer = ByteBuffer.wrap(handleTopicPartitionsRequest(correlationId, topicName, topicUUID));
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
            message.put((byte) 3) // compat arrays = (NUMApiKeys - 1) = now 2 arrays
				  

            // First Key byte array - APIVersions
            .putShort((short)key.getCode()) // 1st KEY
	    .putShort((short) 0) // Min version
            .putShort((short) 4) // Max version
            .put((byte) 0) // TAG BUFFER
	
            // Second Key byte array - DESCRIBE_TOPIC_PARTITIONS
            .putShort((short)key.DESCRIBE_TOPIC_PARTITIONS.getCode())
            .putShort((short) 0) // Min Version Description
            .putShort((short) 0) // Max Version Description
            .put((byte) 0) // TAG BUFFER
	
            // END
            .putInt((int) 0) // throttle_time_ms
            .put((byte) 0); // TAG BUFFER
	    ;
	    
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

    public static boolean validateMessage(byte[] message, int expectedCorrelationID){
	    int correlationID = extractCorrelationID(message);
	    return correlationID == expectedCorrelationID;
    }
    public static int extractCorrelationID(byte[] message){
	    ByteBuffer buffer = ByteBuffer.wrap(message);
	    buffer.order(ByteOrder.BIG_ENDIAN);
	    int correlationID = buffer.getInt(4); // After MessageSize
	    return correlationID;
    }
    public static String extractTopicName(ByteBuffer buffer) {
	    /*
	    // Skip positions:
	    buffer.position(buffer.position() + 4); // Skip MessageSize (4 bytes)
	    buffer.position(buffer.position() + 4); // Skip CorrelationID (4 bytes)
	    buffer.position(buffer.position() + 1); // Skip TAG_BUFFER (1 byte)
	    buffer.position(buffer.position() + 4); // Skip THROTTLE_TIME (4 bytes)
	    buffer.position(buffer.position() + 1); // Skip ARRAY_LENGTH (1 byte)
	    buffer.position(buffer.position() + 2); // Skip ERROR_CODE (2 bytes)
	    */
	    buffer.position(16); // TopicName StringLength exact position
	
	    // Read TopicName Length (1 byte)
	    int topicNameLength = Byte.toUnsignedInt(buffer.get());
	
	    // Read TopicName with extracted topicNameLength:
	    byte[] topicNameBytes = new byte[topicNameLength];
	    buffer.get(topicNameBytes);
	
	    // Convertir los bytes a un String:
	    return new String(topicNameBytes, StandardCharsets.UTF_8);
    }
    
    public static byte[] uuidToByteArray(UUID uuid){
    	byte[] byteArray = new byte[16];
	long mostSigBits = uuid.getMostSignificantBits();
	long leastSigBits = uuid.getLeastSignificantBits();

	for(int i = 0; i < 8; i++){
		byteArray[i] =   (byte)((mostSigBits  >> (8*(7-i))) & 0xFF);
		byteArray[8+i] = (byte)((leastSigBits >> (8*(7-i))) & 0xFF);
	}

	return byteArray;
    }


    public static byte[] handleTopicPartitionsRequest(int correlationID, String topicName, UUID topicUUID){
	    ByteBuffer generatedResponse = ByteBuffer.allocate(1024).order(ByteOrder.BIG_ENDIAN);

	    // 1 Save one Int space for messageSize, now undeclared 
	    generatedResponse.putInt(0);
	    // 2 Write the rest
	    generatedResponse.putInt(correlationID);
	    generatedResponse.put((byte) 0 ); // Header TAG_BUFFER
	    generatedResponse.putInt(0); // Thottle Time
	    generatedResponse.put((byte) 2); // Array Length : N + 1 = 1
	    generatedResponse.putShort((short) 3); // Error Code
	    generatedResponse.put((byte) (topicName.length() + 1)); // TopicNameLength : N+1
	    generatedResponse.put(topicName.getBytes(StandardCharsets.UTF_8)); // TopicName
	    generatedResponse.put(uuidToByteArray(topicUUID)); // TopicID - 16bytes UUID
	    generatedResponse.put((byte) 0); // IsInternal - byte boolean
	    generatedResponse.put((byte) 1); // Partitions Array - COMPACT_ARRAY - N+1=0
	    generatedResponse.putInt(3576); // TopicAuthorizedOperations : 4bytes BitField
	    generatedResponse.put((byte) 0); // Topics TAG_BUFFER
	    generatedResponse.put((byte) 0xFF); // Cursor
	    generatedResponse.put((byte) 0); // Final TAG_BUFFER

	    // 3 calculate MessageSize value and rewrite the first 4 bytes with it
	    int messageSize = generatedResponse.position() - 4;
	    generatedResponse.putInt(0, messageSize);
	    
	    return generatedResponse.array();
	    //return generatedResponse;
    }

}



