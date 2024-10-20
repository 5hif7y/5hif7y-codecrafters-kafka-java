import java.io.InputStream;
import java.io.OutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.Arrays;
import java.nio.BufferUnderflowException;

public class MessageUtils {

    public static int readMessageLength(InputStream reader) throws IOException {
        byte[] lenWord = new byte[4];
        if (reader.read(lenWord) == 4) {
            ByteBuffer buffer = wrap(lenWord);
	    int messageLength = buffer.getInt();
	    System.out.println("Message Length: " + messageLength);
            return messageLength;
        } else {
            System.err.println("Message length not available");
            return -1;
        }
    }

    public static byte[] readMessage(InputStream reader, int messageLength) throws IOException {

	    System.out.println("Message length when 'readMessage' method is applied: " +
			    messageLength);
	    byte[] message = new byte[messageLength];
	    /*
    	    if (reader.read(message) != messageLength) {
            	System.err.println("Message length does not match");
            	return null;
    	    }
	    */

	    int bytesRead = 0;
	    int totalBytesRead = 0;
	
	    while (totalBytesRead < messageLength) {
	        bytesRead = reader.read(message, totalBytesRead, messageLength - totalBytesRead);
	        if (bytesRead == -1) {
	            System.err.println("End of stream reached before reading full message.");
	            return null;
	        }
	        totalBytesRead += bytesRead;
	    }

	    System.out.println("Message readed");
	    
	    
        return message;
    }

    public static void processMessage(OutputStream writer, byte[] message) throws IOException {

        ByteBuffer buffer = wrap(message);
	/*
	    System.out.println("ByteBuffer contents check: ");
	    System.out.println("ByteBuffer DEC: ");
	    printByteBuffer(buffer);
	    System.out.println("ByteBuffer HEX: ");
	    printByteBufferHEX(buffer);
	*/
        //RequestKey key = RequestKey.fromValue(buffer.getShort());
	//short key = (short)APIKeys.RespondAPIKeyRequest(buffer.getShort());
	//
	// Check Buffer status
	buffer.rewind(); // Reset position
	System.out.println("ByteBuffer position: " + buffer.position());
	System.out.println("ByteBuffer limit: " + buffer.limit());
	//
	while(buffer.hasRemaining()){
		byte b = buffer.get();
		System.out.print(b + " ");
	}
	if(buffer.remaining() > 0){
		System.err.println("Not all data was processed. Remaining: " + buffer.remaining());
	}
	System.out.println("All bytes processed.");
	buffer.rewind(); // Reset position
	

	//
	APIKeys key = APIKeys.fromApiKey(buffer.getShort());
        int version = buffer.getShort();
        int correlationId = buffer.getInt();
        System.out.println("Received request for " + key + " " + version + " " + correlationId);
	short clientIDLength = buffer.getShort();
	System.out.println("Client ID Length: " + clientIDLength);
	String clientID = getString(buffer, clientIDLength);
	System.out.println("Client ID: " + clientID);
	System.out.println("Buffer position after clientID: " + buffer.position());
	byte clientTAGBUFFER = buffer.get();
	System.out.println("Buffer position after clientIDTAGBUFFER: " + buffer.position());

        ByteBuffer responseBuffer = null;
        switch (key) {
            case APIKeys.API_VERSIONS:
                responseBuffer = handleApiVersions(version, correlationId, key);
                break;
	    case APIKeys.DESCRIBE_TOPIC_PARTITIONS:
		//buffer.rewind(); // Reset position
		//String topicName = extractTopicName(buffer, (14 + clientID.length() + 1 + 1 + 1)); FIX THIS
		//buffer.position(25);
		byte arrayLength = buffer.get();
		System.out.println("Array length: " + arrayLength);
		byte topicNameLength = buffer.get();
		System.out.println("TopicName length: " + topicNameLength);
		String topicName = getString(buffer, (topicNameLength - 1));
		System.out.println("TopicName: " + topicName);
		UUID topicUUID = UUID.fromString("00000000-0000-0000-0000-000000000000");
		responseBuffer = handleTopicPartitionsRequest(correlationId, topicName, topicUUID);

		if (responseBuffer == null) {
		    System.out.println("Error: responseBuffer is null.");
		} else {
		    System.out.println("Response buffer allocated, size: " + responseBuffer.capacity());
		}

		break;
            // Handle other cases (PRODUCE, FETCH, HEARTBEAT)
	    default:
		System.out.println("Unknown API Key: " + key);
		// implement send error message to client
		//responseBuffer = createErrorResponse(correlationId, "Unknown API Key");
        }

	try{
	        if (responseBuffer != null) {
		    	System.out.println("Response contents: " + Arrays.toString(data(responseBuffer)));
            		writer.write(data(responseBuffer));
          	  	writer.flush();
			System.out.println("Response sent successfully.");
		} else {
			System.out.println("Response buffer is null, nothing to send.");
		}

	} catch (IOException e) {
		System.out.println("Error writing response: " + e.getMessage());
		e.printStackTrace();
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
    // Modding
    //public static String extractTopicName(ByteBuffer buffer, int byteposition) {
	public static String extractTopicName(ByteBuffer buffer, int position) {
	    try {
	        // Guardar la posición actual del buffer para restaurarla luego de la impresión
	        int originalPosition = buffer.position();
	
	        // Marcar la posición actual para restaurarla luego
	        buffer.mark();
	
	        // Imprimir información sobre la posición y el contenido del buffer
	        System.out.println("\nTopicName extraction method:");
	        System.out.println("ByteBuffer position (before extraction): " + buffer.position());
	        System.out.println("ByteBuffer limit: " + buffer.limit());
	
	        // Imprimir contenido del buffer desde la posición actual hasta el final
	        System.out.println("ByteBuffer contents check:");
	        System.out.println("ByteBuffer DEC:");
	        printByteBuffer(buffer); // Imprimir sin modificar la posición
	        System.out.println("ByteBuffer HEX:");
	        printByteBufferHEX(buffer); // Aquí parece que se corta el flujo, entonces lo depuramos más
					    //
		buffer.rewind();
	        System.out.println("Position of the byte extraction: " + position);
	        System.out.println("byte extraction: " + buffer.get(position));
	
	        // Restaurar la posición original
	        buffer.reset();
	
	        // Verificar si la posición es válida antes de leer
	        if (position >= buffer.limit()) {
	            System.out.println("Error: position is out of the buffer's limit.");
	            return null;
	        }
	
	        // Set the position manually to the exact point where the topic name starts
	        buffer.position(position);  // Mover el cursor a la posición indicada
	        System.out.println("Position set to: " + buffer.position());
	
	        // Verificar si hay suficientes bytes restantes para leer la longitud del nombre
	        if (buffer.remaining() < 1) {
	            System.out.println("Error: not enough bytes remaining in the buffer to read the topic name length.");
	            return null;
	        }
	
	        // Leer la longitud del nombre del tópico (1 byte)
	        short topicNameLength = (short) (buffer.get() & 0xFF);  // Leer longitud del nombre
	        System.out.println("TopicName string length: " + topicNameLength);
	
	        // Verificar la longitud del nombre del tópico
	        if (topicNameLength <= 0 || topicNameLength > buffer.remaining()) {
	            System.out.println("Error: Invalid or inconsistent topic name length. Length: " + topicNameLength);
	            return null;
	        }
	
	        // Verificar si hay suficientes bytes restantes para leer el nombre del tópico
	        if (buffer.remaining() < topicNameLength) {
	            System.out.println("Error: not enough bytes remaining to read the topic name.");
	            return null;
	        }
	
	        // Leer el nombre del tópico basado en la longitud extraída
	        byte[] topicNameBytes = new byte[topicNameLength];
	        buffer.get(topicNameBytes);  // Extraer los bytes para el nombre del tópico
	
	        // Convertir los bytes a String y devolver
	        return new String(topicNameBytes, StandardCharsets.UTF_8);
	
	    } catch (BufferUnderflowException e) {
	        System.err.println("BufferUnderflowException occurred: " + e.getMessage());
	        e.printStackTrace();
	        return null;
	    } catch (Exception e) {
	        System.err.println("An exception occurred: " + e.getMessage());
	        e.printStackTrace();
	        return null;
	    }
	}
 

    // End Modding


    
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


    public static ByteBuffer handleTopicPartitionsRequest(int correlationID, String topicName, UUID topicUUID){
	    ByteBuffer message = createTopicPartitionsResponse(correlationID, topicName, topicUUID);
	    return createResponseBuffer(message);

    }
    public static ByteBuffer createTopicPartitionsResponse(int correlationID, String topicName, UUID topicUUID){

	    ByteBuffer message = ByteBuffer.allocate(1024).order(ByteOrder.BIG_ENDIAN);

	    message.putInt(correlationID);
	    message.put((byte) 0 ); // Header TAG_BUFFER
	    message.putInt(0); // Thottle Time
	    message.put((byte) 2); // Array Length : N + 1 = 1
	    //message.putShort((short) 3); // Error Code - UNKNOWN_TOPIC add TopicsErrorsEnum
	    message.putShort((short) 0); // Error Code - None add TopicsErrorsEnum
	    message.put((byte) (topicName.length() + 1)); // TopicNameLength : N+1
	    message.put(topicName.getBytes(StandardCharsets.UTF_8)); // TopicName
	    message.put(uuidToByteArray(topicUUID)); // TopicID - 16bytes UUID
	    message.put((byte) 0); // IsInternal - byte boolean
	    message.put((byte) 1); // Partitions Array - COMPACT_ARRAY - N+1=0
	    message.putInt(3576); // TopicAuthorizedOperations : 4bytes BitField
	    message.put((byte) 0); // Topics TAG_BUFFER
	    message.put((byte) 0xFF); // Cursor
	    message.put((byte) 0); // Final TAG_BUFFER
	    
	    return message;
    }

    public static String getString(ByteBuffer buffer, int N) {

	    if(buffer.remaining() < N){
		    throw new IllegalArgumentException("Insufficient bytes in buffer.");
	    }

	    byte[] bytes = new byte[N];
	    buffer.get(bytes);

	    return new String(bytes, StandardCharsets.UTF_8);
    }

    public static void printByteBuffer(ByteBuffer buffer) {
    	buffer.flip();
        
    	byte[] bytes = new byte[buffer.remaining()];
    	buffer.get(bytes);
    	
    	for (byte b : bytes) {
    	    System.out.print(b + " ");
    	}
    	System.out.println();
    }
    public static void printByteBufferHEX(ByteBuffer buffer) {
    	buffer.flip();
        
    	byte[] bytes = new byte[buffer.remaining()];
    	buffer.get(bytes);
    	
    	for (byte b : bytes) {
    	    System.out.printf("%02X ", b & 0xFF);
    	}
    	System.out.println();
    }
}



