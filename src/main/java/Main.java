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
       /*
       */
       ByteBuffer outputBuffer = ByteBuffer.allocate(128);
       processOutput(outputBuffer, message_size, correlation_id, request_api_key,
		       request_api_version, client_id, tagged_fields);
       out.write(outputBuffer.array(), 0, outputBuffer.position());

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


	  outputBuffer.putInt(ByteBuffer.wrap(message_size).getInt());
	  outputBuffer.putShort(ByteBuffer.wrap(request_api_key).getShort());
	  short version = ByteBuffer.wrap(request_api_version).getShort();
	  if(version < 0 || version > 4){
		  outputBuffer.putShort(ByteBuffer.wrap(new byte[] {0, 35}).getShort());
	  } else {
		  outputBuffer.putShort(ByteBuffer.wrap(request_api_version).getShort());
	  }
	  outputBuffer.putInt(ByteBuffer.wrap(correlation_id).getInt());
	  outputBuffer.putShort(ByteBuffer.wrap(client_id).getShort());
	  outputBuffer.put(tagged_fields[0]);



	
  }
}
