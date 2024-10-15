import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class CircularBuffer {

	private byte[][] buffer;
	private int size;
	private int head;
	private int tail;
	private int count;
	//private ReentrantLock lock; // Lock for concurrency control
	private Lock lock; // Lock for concurrency control

	public CircularBuffer(int size) {
		this.size = size;
		this.buffer = new byte[size][];
		this.head = 0;
		this.tail = 0;
		this.count = 0;
		this.lock = new ReentrantLock(); // Lock for concurrency control
	}

	// Add message to buffer (Producer)
	public void add(byte[] message) {
		lock.lock();
		try {
			// Overwrite oldest if full
			if (count == size) {
				head = (head + 1) % size; // Move head forward when overwriting
			} else {
				count++; // Normal behavior, no overwriting
			}
			buffer[tail] = message;
			tail = (tail + 1) % size; // Move tail to the next empty spot
		} finally {
			lock.unlock(); // End
		}
	}

	// Retrieve message from buffer (Consumer
	public byte[] get() {
		lock.lock();
		try {
			if(count == 0){
				return null; // Buffer empty
			}
			byte[] message = buffer[head];
			buffer[head] = null; // Clear slot
			head = (head + 1) % size; // Move head forward
			count--;
			return message;
		} finally {
			lock.unlock();
		}
	}

	// Checking method funcs
	public boolean isEmpty() {
		return count == 0;
	}

	public boolean isFull(){
		return count == size;
	}
}
	
