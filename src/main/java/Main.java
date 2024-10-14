import java.io.IOException;
import java.io.OutputStream;
import java.io.InputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;

public class Main {
  public static void main(String[] args){

    System.err.println("Logs from your program will appear here!");

     ServerSocket serverSocket = null;
     Socket clientSocket = null;
     int port = 9092;
     try {
	     serverSocket = new ServerSocket(port);
	     serverSocket.setReuseAddress(true);
	     System.out.println("Server listening on port " + port);
	     
	     // Wait for connection from client
	     while(true){
		     clientSocket = serverSocket.accept();
		     handleClient(clientSocket);
	     }

       } catch (IOException e) {
	     System.out.println("IOException: " + e.getMessage());
     } finally {
	     try {
		     if (clientSocket != null) {
			     clientSocket.close();
		     }
		     if (serverSocket != null) {
			     serverSocket.close();
		     }
	     } catch (IOException e) {
		     System.out.println("IOException: " + e.getMessage());
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
			  if (in.available() > 0){

				  // Input
				  byte[] message_size = in.readNBytes(4); // INT32, 4 bytes
				  if(message_size.length == 0) break;  // --- break while loop conditional;
				  byte[] request_api_key = in.readNBytes(2); // INT16, 2 bytes
				  byte[] request_api_version = in.readNBytes(2); // INT16, 2 bytes
				  byte[] correlation_id = in.readNBytes(4); //INT32, 4 bytes
				  // client_id --- Nullable String ---
				  // Fixed to MAX INT16, 2 bytes, not sure if max INT64, 8 bytes
				  short client_id_length = ByteBuffer.wrap(in.readNBytes(2)).getShort();
				  byte[] client_id = client_id_length > 0 ? in.readNBytes(client_id_length) : new byte[0];
				  byte[] tagged_fields = in.readNBytes(1); // TAGGED_FIELDS;

				  // Output
				  ByteBuffer response = generateResponse(message_size, request_api_key, request_api_version, correlation_id, client_id, tagged_fields);
				  out.write(response.array());
				  out.flush(); // not sure if this is neccesary
			}
			// test if the client closed the connection to the server
			if(clientSocket.isClosed() || in.read() == -1){
				break;
			}
		  }
	  } catch (IOException e){
		  System.out.println("IOException while handling client: " + e.getMessage());
	  }
  }


  private static ByteBuffer generateResponse(byte[] message_size, byte[] request_api_key, byte[] request_api_version, byte[] correlation_id, byte[] client_id, byte[] tagged_fields){

	  // Temp buffer
	  ByteBuffer tempBuffer = ByteBuffer.allocate(128);
	  processOutput(tempBuffer, message_size, request_api_key, request_api_version, correlation_id, client_id, tagged_fields);

	  // Estimate total size of the message, but reserving the first 4 bytes
	  int response_size = tempBuffer.position();
	  ByteBuffer OutputBuffer = ByteBuffer.allocate(4 + response_size);
	  OutputBuffer.putInt(response_size);
	  OutputBuffer.put(tempBuffer.array(), 0, response_size);

	  return OutputBuffer;
  }

  private static void processOutput(ByteBuffer outputBuffer, byte[] message_size, byte[] request_api_key, byte[] request_api_version, byte[] correlation_id, byte[] client_id, byte[] tagged_fields){

	  // Correlation ID --- INT32, 4 bytes, int
	  outputBuffer.putInt(ByteBuffer.wrap(correlation_id).getInt());
	  // API_VERSION --- INT16, 2 bytes, short
	  // Error code --- INT16, 2 bytes, short --- 0 means "No Error"
	  short version = ByteBuffer.wrap(request_api_version).getShort();
	  if(version < 0 || version > 4){
		  outputBuffer.putShort((short)35);
	  } else {
		  outputBuffer.putShort((short)0);
	  }
	  // Response header --- 1 byte --- 2
	  outputBuffer.put((byte) 2);
	  // API_KEY --- INT16, 2bytes, short --- 18
	  outputBuffer.putShort((short) 18);
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
