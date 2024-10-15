import java.io.IOException;
import java.io.OutputStream;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;

public class KafkaProtocolUtils {

  //public static void processBufferResponses(CircularBuffer<byte[]> circularBuffer, OutputStream out){
  public static void processBufferResponses(CircularBuffer circularBuffer, OutputStream out){
	  while(!circularBuffer.isEmpty()){

		  byte[] bufferData = circularBuffer.get();

		  if(bufferData != null){
			  ByteBuffer response = generateResponse(bufferData);

			  try {
				  out.write(response.array());
				  out.flush();
			  } catch (IOException e) {
				  System.out.println("Error writing to output stream: " + e.getMessage());
			  }
		  }
	  }
  }


  // Consumer: Process messages from the buffer (Output?)
  public static void consumeMessages(CircularBuffer messageBuffer){
	  while(true){
		  try {
			  byte[] message = messageBuffer.get();
			  if(message != null){
				  System.out.println("Processing message: " + Arrays.toString(message));
			  }
			  Thread.sleep(100); // Sleep 0.1 s
		  } catch (InterruptedException e){
			  System.out.println("Interrupted while consuming messages: " + e.getMessage());
		  }
		  /*
		  } catch (IOException e){
			  System.out.println("Interrupted while consuming messages: " + e.getMessage());
		  }*/
	  }
  }

  public static byte[] readExactly(InputStream in, int numBytes) throws IOException {
	  byte[] buffer = new byte[numBytes];
	  int bytesRead = 0;
	  while(bytesRead < numBytes){
		  int result = in.read(buffer, bytesRead, numBytes - bytesRead);
		  if(result == -1){
			  throw new IOException("Unexpected end of stream");
		  }
		  bytesRead += result;
	  }
	  return buffer;
  }

  public static void logRequest(byte[] message_size, byte[] request_api_key, byte[] request_api_version, byte[] correlation_id, short client_id_length, byte[] client_id, byte[] tagged_fields) {

	System.out.println("Received message size: " + ByteBuffer.wrap(message_size).getInt());
	System.out.println("Request API Key: " + Arrays.toString(request_api_key));
	System.out.println("Request API Version: " + Arrays.toString(request_api_version));
	System.out.println("Correlation ID: " + Arrays.toString(correlation_id));
	System.out.println("Client ID Length: " + client_id_length);
	System.out.println("Client ID byte representation: " + Arrays.toString(client_id));
	System.out.println("Client ID String representation: " + new String(client_id));
	System.out.println("Tagged Fields: " + Arrays.toString(tagged_fields));
  }


  // Reimplementing response processing
  //private static ByteBuffer generateResponse(byte[] message_size, byte[] request_api_key, byte[] request_api_version, byte[] correlation_id, byte[] client_id, byte[] tagged_fields){
  //processOutput(tempBuffer, message_size, request_api_key, request_api_version, correlation_id, client_id, tagged_fields);
  public static ByteBuffer generateResponse(byte[] bufferData){

	  // Input buffer to read data from bufferData
	  ByteBuffer inputBuffer = ByteBuffer.wrap(bufferData);

	  // Extract fields from bufferData
	  // message_size --- INT32, 4 bytes, int
	  byte[] message_size = new byte[4];
	  inputBuffer.get(message_size);

	  // request_api_key --- INT16, 2 bytes, short
	  byte[] request_api_key = new byte[2];
	  inputBuffer.get(request_api_key);
	  // request_api_version --- INT16, 2 bytes, short
	  byte[] request_api_version = new byte[2];
	  inputBuffer.get(request_api_version);

	  // correlation_id --- INT32, 4 bytes, int
	  byte[] correlation_id = new byte[4];
	  inputBuffer.get(correlation_id);

	  // client_id --- Nullable String --- MIN: INT16, 2 bytes , short --- MAX: INT64, 8 bytes
	  int client_id_length = inputBuffer.getInt();
	  byte[] client_id = new byte[client_id_length];
	  inputBuffer.get(client_id);

	  // tagged_fields --- Nullable String --- MIN: INT16, 2 bytes , short --- MAX: INT64, 8 bytes
	  int tagged_fields_length = inputBuffer.getInt();
	  byte[] tagged_fields = new byte[client_id_length];
	  inputBuffer.get(tagged_fields);

	  // Tempbuffer
	  ByteBuffer tempBuffer = ByteBuffer.allocate(128);
	  processOutput(tempBuffer, message_size, request_api_key, request_api_version, correlation_id);

	  // Estimate total size of the message, but reserving the first 4 bytes
	  int response_size = tempBuffer.position();
	  // try to process that first 4 bytes
	  ByteBuffer OutputBuffer = ByteBuffer.allocate(4 + response_size);
	  OutputBuffer.putInt(response_size);
	  OutputBuffer.put(tempBuffer.array(), 0, response_size);

	  return OutputBuffer;
  }

  private static short RespondAPIVersionRequest(byte[] request_api_version){
	  short version = ByteBuffer.wrap(request_api_version).getShort();
	  ErrorCodes errorCode = ErrorCodes.fromApiVersion(version);
	  return (short)errorCode.getCode();
  }
  private static short RespondAPIKeyRequest(byte[] request_api_key){
	  short apiKey = ByteBuffer.wrap(request_api_key).getShort();
	  APIKeys apiKeyEnum = APIKeys.fromApiKey(apiKey);
	  return (short)apiKeyEnum.getCode();
  }

  //private static void processOutput(ByteBuffer outputBuffer, byte[] message_size, byte[] request_api_key, byte[] request_api_version, byte[] correlation_id, byte[] client_id, byte[] tagged_fields){
  public static void processOutput(ByteBuffer outputBuffer, byte[] message_size, byte[] request_api_key, byte[] request_api_version, byte[] correlation_id){

	  // Correlation ID --- INT32, 4 bytes, int
	  outputBuffer.putInt(ByteBuffer.wrap(correlation_id).getInt());
	  // API_VERSION --- INT16, 2 bytes, short
	  // Error code --- INT16, 2 bytes, short --- 0 means "No Error"
	  outputBuffer.putShort((short)RespondAPIVersionRequest(request_api_version));
	  // Testing if this is not ok
	  // Response header --- 1 byte --- 2
	  outputBuffer.put((byte) 2);
	  // API_KEY --- INT16, 2bytes, short --- 18
	  outputBuffer.putShort((short)RespondAPIKeyRequest(request_api_key));

	  // MIN_VERSION --- INT16, 2bytes, short --- 0
	  outputBuffer.putShort((short) 0);
	  // MAX_VERSION --- INT16, 2 bytes, short --- 4
	  outputBuffer.putShort((short) 4);
	  // Throttle time --- INT32, 4 bytes --- 0
	  outputBuffer.putInt((int) 0);
	  // TAGGED_FIELDS --- INT16, 2 bytes --- now 0 'no tagged fields' --- not sure if MAX INT64
	  outputBuffer.putShort((short) 0); 
  }
}
