import java.io.IOException;
import java.io.OutputStream;
import java.io.InputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.Arrays;
import java.util.HexFormat;


public class Main {

	private static int PORT = 9092;
	private static int THREAD_POOL_SIZE = 4;
	private static int SOCKET_TIMEOUT_MS = 9000; // 9 seconds, 1 second less than test
	
	// Recoupling to CircularBuffer structure
	// CircularBuffer implementation
	private static int BUFFER_SIZE = 100;
	private static CircularBuffer messageBuffer = new CircularBuffer(BUFFER_SIZE);

  public static void main(String[] args){

     System.err.println("Logs from your program will appear here!");

     try {
	     ServerSocket serverSocket = new ServerSocket(PORT);
	     ExecutorService executorService = Executors.newFixedThreadPool(THREAD_POOL_SIZE);
	
	     if(serverSocket != null){
		     handleConnections(serverSocket, executorService);
     	     }
     } catch (IOException e) {
	     System.err.println("Error creating ServerSocket: " + e.getMessage());
     }
  }


  private static ServerSocket createServerSocket(int port){
	  try{
		  ServerSocket serverSocket = new ServerSocket(port);
		  serverSocket.setReuseAddress(true);
		  return serverSocket;
	  } catch (IOException e) {
		  System.err.println("Failed to create server socket: " + e.getMessage());
		  return null;
	  }
  }

  private static void handleConnections(ServerSocket serverSocket, ExecutorService executorService){
	  try{
		  while(true){
			  Socket clientSocket = serverSocket.accept();
			  System.out.println("Accepted connection from " + clientSocket.getRemoteSocketAddress());
			  executorService.submit(() -> KafkaClientHandler(clientSocket));
		  }
	  } catch (IOException e) {
		  System.err.println("Error handling connections: " + e.getMessage());
	  } finally {
		  closeServerSocket(serverSocket);
	  }
  }

  private static void closeServerSocket(ServerSocket serverSocket){
	  if (serverSocket != null){
		  try{
			  serverSocket.close();
		  } catch (IOException e){
			  System.err.println("Failed to close server socket: " + e.getMessage());
		  }
	  }
  }

  

  private static void KafkaClientHandler(Socket clientSocket) {

	  HexFormat hexFormat = HexFormat.of();

	  try (InputStream reader = clientSocket.getInputStream();
	       OutputStream writer = clientSocket.getOutputStream()){

		while(true) {
			int messageLength = MessageUtils.readMessageLength(reader);
			if(messageLength <= 0) break;

			byte[] message = MessageUtils.readMessage(reader, messageLength);
			if(message != null){

				// Add the message to the circular buffer
				messageBuffer.add(message);
				// Process the message from the circular buffer
				byte[] bufferedMessage = messageBuffer.get();
				if(bufferedMessage != null){
					MessageUtils.processMessage(writer, bufferedMessage);
				}
			}
		}
	} catch (IOException e){
		System.err.println("Error while handling client: " + e.getMessage());
	} finally {
		closeClientSocket(clientSocket);
	}
  }

  private static void closeClientSocket(Socket clientSocket) {
	  try{
		  System.out.println("Connection from " + clientSocket.getRemoteSocketAddress() + " ended");
		  clientSocket.close();
	  } catch (IOException e) {
		  System.err.println("Error closing client socket: " + e.getMessage());
	  }
  }

}

