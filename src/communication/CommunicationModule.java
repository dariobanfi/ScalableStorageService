package communication;


import java.io.*;
import java.net.*;

import org.apache.log4j.Logger;
/**
 * @author Dario
 * Communication module which handles basic functions like connecting, sending bytes,
 * receiving bytes and disconnecting
 */
public class CommunicationModule {
	
	private Logger logger = Logger.getRootLogger();
	private boolean running;
	private String address;
	private int port;
	private Socket clientSocket;
	private OutputStream output;
 	private InputStream input;
	private static final int BUFFER_SIZE = 1024;
	private static final int DROP_SIZE = 1024 * BUFFER_SIZE;
	/**
	 * Initialize Communication module with address and port of a server
	 * @param address the address of the server
	 * @param port the port of the server
	 * @throws IOException 
	 * @throws UnknownHostException 
	 */
	public CommunicationModule(String address, int port)  {	
		this.address = address;
		this.port = port;
	}

	public void connect() throws UnknownHostException, IOException {
			clientSocket = new Socket(this.address, this.port);
			setRunning(true);
			output = clientSocket.getOutputStream();
			input = clientSocket.getInputStream();
			logger.info("Connection established");
	}


	public void sendBytes(byte[] bytes) throws IOException {
		output.write(bytes, 0, bytes.length);
		output.flush();
		logger.info("Sent " + bytes.length + " bytes");
    }
	
	public byte[] receiveBytes() throws IOException {
		
		int index = 0;
		byte[] msgBytes = null, tmp = null;
		byte[] bufferBytes = new byte[BUFFER_SIZE];
		
		/* read first char from stream */
		byte read = (byte) input.read();	
		boolean reading = true;
		
		while(read != 13 && reading) {/* carriage return */
			/* if buffer filled, copy to msg array */
			if(index == BUFFER_SIZE) {
				if(msgBytes == null){
					tmp = new byte[BUFFER_SIZE];
					System.arraycopy(bufferBytes, 0, tmp, 0, BUFFER_SIZE);
				} else {
					tmp = new byte[msgBytes.length + BUFFER_SIZE];
					System.arraycopy(msgBytes, 0, tmp, 0, msgBytes.length);
					System.arraycopy(bufferBytes, 0, tmp, msgBytes.length,
							BUFFER_SIZE);
				}

				msgBytes = tmp;
				bufferBytes = new byte[BUFFER_SIZE];
				index = 0;
			} 
			
			/* only read valid characters, i.e. letters and constants */
			bufferBytes[index] = read;
			index++;
			
			/* stop reading is DROP_SIZE is reached */
			if(msgBytes != null && msgBytes.length + index >= DROP_SIZE) {
				reading = false;
			}
			
			/* read next char from stream */
			read = (byte) input.read();
		}
		
		if(msgBytes == null){
			tmp = new byte[index];
			System.arraycopy(bufferBytes, 0, tmp, 0, index);
		} else {
			tmp = new byte[msgBytes.length + index];
			System.arraycopy(msgBytes, 0, tmp, 0, msgBytes.length);
			System.arraycopy(bufferBytes, 0, tmp, msgBytes.length, index);
		}
		
		msgBytes = tmp;
		
		return msgBytes;
    }
	
	private void setRunning(boolean val){
		this.running = val;
	}

	public boolean isRunning(){
		return this.running;
	}
	
	public void closeConnection() throws IOException {
		setRunning(false);
		logger.info("Disconnecting");
		if (clientSocket != null) {
			input.close();
			output.close();
			clientSocket.close();
			clientSocket = null;
			logger.info("connection closed!");
		}
	}
	
}