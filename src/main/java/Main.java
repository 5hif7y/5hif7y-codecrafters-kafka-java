import java.io.IOException;
import java.io.OutputStream;
import java.io.InputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;

public class Main {
  public static void main(String[] args){
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    System.err.println("Logs from your program will appear here!");

    // Uncomment this block to pass the first stage
    // 
     ServerSocket serverSocket = null;
     Socket clientSocket = null;
     int port = 9092;
     try {
       serverSocket = new ServerSocket(port);
       // Since the tester restarts your program quite often, setting SO_REUSEADDR
       // ensures that we don't run into 'Address already in use' errors
       serverSocket.setReuseAddress(true);
       // Wait for connection from client.
       clientSocket = serverSocket.accept();

       // Input
       InputStream in = clientSocket.getInputStream();
       byte[] message_size = in.readNBytes(4); // INT32, 4 bytes
       byte[] request_api_key = in.readNBytes(2); // INT16, 2 bytes
       byte[] request_api_version = in.readNBytes(2); // INT16, 2 bytes
       byte[] correlation_id = in.readNBytes(4); //INT32, 4 bytes
       // client_id --- Nullable String --- Fixed to MAX INT16, 2 bytes, not sure if max INT64, 8 bytes
       short client_id_length = ByteBuffer.wrap(in.readNBytes(2)).getShort();
       byte[] client_id = client_id_length > 0 ? in.readNBytes(client_id_length) : new byte[0];
       byte[] tagged_fields = in.readNBytes(1); // TAGGED_FIELDS

       // Output
       OutputStream out = clientSocket.getOutputStream();
       
       ByteBuffer outputBuffer = ByteBuffer.allocate(128);
       processOutput(outputBuffer, message_size, request_api_key, request_api_version, correlation_id, client_id, tagged_fields);

       // Calc total size of the message, excluding the first 4 bytes
       int response_size = outputBuffer.position();
       ByteBuffer finalOutputBuffer = ByteBuffer.allocate(4 + response_size);
       finalOutputBuffer.putInt(response_size);
       finalOutputBuffer.put(outputBuffer.array(), 0, response_size);

       out.write(finalOutputBuffer.array());

     } catch (IOException e) {
       System.out.println("IOException: " + e.getMessage());
     } finally {
       try {
         if (clientSocket != null) {
           clientSocket.close();
         }
       } catch (IOException e) {
         System.out.println("IOException: " + e.getMessage());
       }
     }
  }

  private static void processOutput(ByteBuffer outputBuffer, byte[] message_size, byte[] request_api_key, byte[] request_api_version, byte[] correlation_id, byte[] client_id, byte[] tagged_fields){

	  // Correlation ID --- INT32, 4 bytes, int
	  outputBuffer.put(correlation_id);
	  // API_VERSION --- INT16, 2 bytes, short
	  // Error code --- INT16, 2 bytes, short --- 0 means "No Error"
	  short version = ByteBuffer.wrap(request_api_version).getShort();
	  if(version < 0 || version > 4){
		  outputBuffer.putShort((short)35);
	  } else {
		  outputBuffer.putShort((short)0);
	  }
	  // I have no idea what this is, spacing?
	  outputBuffer.put((byte) 2);
	  // API_KEY --- INT16, 2bytes, short --- 18
	  outputBuffer.putShort((short) 18);
	  // MIN_VERSION --- INT16, 2bytes, short --- '0'
	  outputBuffer.putShort((short) 0);
	  // MAX_VERSION --- INT16, 2bytes, short --- '4'
	  outputBuffer.putShort((short) 4);
	  // TAGGED_FIELDS --- INT32, 4 bytes, int --- now '0' --- not sure if MAX INT64
	  outputBuffer.putInt(0);
	
  }
}
