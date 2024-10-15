import java.io.IOException;
import java.io.OutputStream;
import java.io.InputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class Main {

	private static int PORT = 9092;
	private static int THREAD_POOL_SIZE = 4;
	private static int SOCKET_TIMEOUT_MS = 9000; // 9 seconds, 1 second less than test

  public static void main(String[] args){

    System.err.println("Logs from your program will appear here!");


     ExecutorService executorService = Executors.newFixedThreadPool(THREAD_POOL_SIZE);
     try (ServerSocket serverSocket = new ServerSocket(PORT)){
	     serverSocket.setReuseAddress(true);
	     System.out.println("Server listening on port " + PORT);
	     
	     // Wait for connection from client
	     while(true){
		     Socket clientSocket = serverSocket.accept();
		     clientSocket.setSoTimeout(SOCKET_TIMEOUT_MS);
		     executorService.submit(() -> handleClient(clientSocket));
	     }

       } catch (IOException e) {
	     System.out.println("IOException: " + e.getMessage());
     } finally {
	     executorService.shutdown();
	     try {
		     if(!executorService.awaitTermination(60, TimeUnit.SECONDS)){
			     executorService.shutdownNow();
		     }
	     } catch (InterruptedException ex){
		     executorService.shutdownNow();
	     }
     }
  }

  private static void handleClient(Socket clientSocket){

	  try {
		  // Input/Output streams
		  InputStream in = clientSocket.getInputStream();
		  OutputStream out = clientSocket.getOutputStream();

		  // Loop to behave like a server
		  while(true){

			  // Input
			  byte[] message_size = readExactly(in, 4); // INT32, 4 bytes
			  if(message_size.length == 0) break;  // --- break while loop conditional;
			  byte[] request_api_key = readExactly(in, 2); // INT16, 2 bytes
			  byte[] request_api_version = readExactly(in, 2); // INT16, 2 bytes
			  byte[] correlation_id = readExactly(in, 4); //INT32, 4 bytes
			  // client_id --- Nullable String ---
			  // Fixed to MAX INT16, 2 bytes, not sure if max INT64, 8 bytes
			  short client_id_length = ByteBuffer.wrap(in.readNBytes(2)).getShort();
			  byte[] client_id = client_id_length > 0 ? in.readNBytes(client_id_length) : new byte[0];
			  byte[] tagged_fields = in.readNBytes(1); // TAGGED_FIELDS;
			  
			  // Print input
			  logRequest(message_size, request_api_key, request_api_version, correlation_id, client_id_length, client_id, tagged_fields);

			  // Output
			  ByteBuffer response = generateResponse(message_size, request_api_key, request_api_version, correlation_id, client_id, tagged_fields);
			  out.write(response.array());
			  out.flush(); // not sure if this is neccesary
		}
	  } catch (IOException e){
		  System.out.println("IOException while handling client: " + e.getMessage());
	  }
  }

  private static byte[] readExactly(InputStream in, int numBytes) throws IOException {
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

  private static void logRequest(byte[] message_size, byte[] request_api_key, byte[] request_api_version, byte[] correlation_id, short client_id_length, byte[] client_id, byte[] tagged_fields) {

	System.out.println("Received message size: " + ByteBuffer.wrap(message_size).getInt());
	System.out.println("Request API Key: " + Arrays.toString(request_api_key));
	System.out.println("Request API Version: " + Arrays.toString(request_api_version));
	System.out.println("Correlation ID: " + Arrays.toString(correlation_id));
	System.out.println("Client ID Length: " + client_id_length);
	System.out.println("Client ID byte representation: " + Arrays.toString(client_id));
	System.out.println("Client ID String representation: " + new String(client_id));
	System.out.println("Tagged Fields: " + Arrays.toString(tagged_fields));
  }


  private static ByteBuffer generateResponse(byte[] message_size, byte[] request_api_key, byte[] request_api_version, byte[] correlation_id, byte[] client_id, byte[] tagged_fields){

	  // Temp buffer
	  ByteBuffer tempBuffer = ByteBuffer.allocate(128);
	  //processOutput(tempBuffer, message_size, request_api_key, request_api_version, correlation_id, client_id, tagged_fields);
	  processOutput(tempBuffer, message_size, request_api_key, request_api_version, correlation_id);

	  // Estimate total size of the message, but reserving the first 4 bytes
	  int response_size = tempBuffer.position();
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
  private static void processOutput(ByteBuffer outputBuffer, byte[] message_size, byte[] request_api_key, byte[] request_api_version, byte[] correlation_id){

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
	  

	  // NEW changes // not ok
	  // Num API KEYS --- INT16, 2 bytes, short --- now 1
	  //outputBuffer.putShort((short) 1); // I think this is expected to be 1
	  // Num API KEYS --- INT16, 2 bytes, short --- 18 now readed from 'request_api_key'
	  //outputBuffer.putShort(ByteBuffer.wrap(request_api_key).getShort());
	  // END NEW changes

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
