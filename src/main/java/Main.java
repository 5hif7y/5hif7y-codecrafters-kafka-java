import java.io.IOException;
import java.io.OutputStream;
import java.io.InputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class Main {

	private static int PORT = 9092;
	private static int THREAD_POOL_SIZE = 4;
	private static int SOCKET_TIMEOUT_MS = 9000; // 9 seconds, 1 second less than test
	
	// CircularBuffer implementation
	private static int BUFFER_SIZE = 100;
	private static CircularBuffer messageBuffer = new CircularBuffer(BUFFER_SIZE);

  public static void main(String[] args){


     System.err.println("Logs from your program will appear here!");

     ExecutorService executorService = Executors.newFixedThreadPool(THREAD_POOL_SIZE);

     try (ServerSocket serverSocket = new ServerSocket(PORT)){
	     serverSocket.setReuseAddress(true);
	     System.out.println("Server listening on port " + PORT);

	     // Start consumer thread to process messages from the circular buffer
	     executorService.submit(() -> KafkaProtocolUtils.consumeMessages(messageBuffer));
	     
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
  
  // Modified for circular buffer implementation
  // Separating input and output behaviors as producer and consumer
  // Producer: Handle client connection and add messages to buffer (Input?)
  private static void handleClient(Socket clientSocket){

	  try {
		  // Input/Output streams
		  InputStream in = clientSocket.getInputStream();
		  OutputStream out = clientSocket.getOutputStream();

		  // Loop to behave like a server
		  while(true){

			  // Input
			  byte[] message_size = KafkaProtocolUtils.readExactly(in, 4); // INT32, 4 bytes
			  if(message_size.length == 0) break;  // --- break while loop conditional;
			  byte[] request_api_key = KafkaProtocolUtils.readExactly(in, 2); // INT16, 2 bytes
			  byte[] request_api_version = KafkaProtocolUtils.readExactly(in, 2); // INT16, 2 bytes
			  byte[] correlation_id = KafkaProtocolUtils.readExactly(in, 4); //INT32, 4 bytes
			  // Confirmed Nullable String datatype : MIN = INT16,2 bytes. MAX = INT64,8 bytes
			  // client_id --- Nullable String fixed to MAX INT16, 2 bytes, short
			  short client_id_length = ByteBuffer.wrap(in.readNBytes(2)).getShort();
			  byte[] client_id = client_id_length > 0 ? in.readNBytes(client_id_length) : new byte[0];
			  // TAGGED_FIELDS --- Nullable String fixed to MAX INT16, 2 bytes, short
			  byte[] tagged_fields = in.readNBytes(1); // TAGGED_FIELDS;
			  
			  // Print input
			  KafkaProtocolUtils.logRequest(message_size, request_api_key, request_api_version, correlation_id, client_id_length, client_id, tagged_fields);

			  // Pack the entire message into a byte array
			  ByteBuffer buffer = ByteBuffer.allocate(
					  4 + 2 + 2 + 4 + 2 + client_id.length + 1
				);
			  buffer.put(message_size);
			  buffer.put(request_api_key);
			  buffer.put(request_api_version);
			  buffer.put(correlation_id);
			  buffer.putShort(client_id_length);
			  buffer.put(client_id);
			  buffer.put(tagged_fields);

			  // Load/Write into circularBuffer
			  messageBuffer.add(buffer.array());

			  // Process and send responses from circularBuffer
			  KafkaProtocolUtils.processBufferResponses(messageBuffer, out);

		}
	  } catch (IOException e){
		  System.out.println("IOException while handling client: " + e.getMessage());
	  }
  }
}

