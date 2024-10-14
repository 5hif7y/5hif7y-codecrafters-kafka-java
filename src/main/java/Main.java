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
       byte[] client_id; // Nullable String
       byte[] tagged_fields; // TAGGED_FIELDS

       // Output
       OutputStream out = clientSocket.getOutputStream();
       out.write(message_size);
       out.write(correlation_id);

       short version = ByteBuffer.wrap(request_api_version).getShort();
       if(version < 0 || version > 4){
	       out.write(new byte[] {0, 35});
       }

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
}
