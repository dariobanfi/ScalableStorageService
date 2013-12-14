package app_kvServer;

import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.util.Map;

import org.apache.log4j.*;

import common.messages.*;


/**
 * @author Dario
 *
 * This class implements Runnable and it represents the thread that gets launched every time
 * a new socket request comes to the server.
 * It receives the database pointer from the server app, in order to do operations on it
 * (with synchronized methods, to avoid problems)
 */
public class ClientConnection implements Runnable {

	private static Logger logger = Logger.getRootLogger();
	
	private boolean isOpen;
	private static final int BUFFER_SIZE = 1024;
	private static final int DROP_SIZE = 128 * BUFFER_SIZE;
	
	private Socket clientSocket;
	private InputStream input;
	private OutputStream output;
	private Map<String, String> database;
	private KVServer Server;
	
	/**
	 * Constructs a new CientConnection object for a given TCP socket.
	 * @param clientSocket the Socket object for the client connection.
	 */
	public ClientConnection(Socket clientSocket, KVServer currentServer) {
		this.clientSocket = clientSocket;
		this.isOpen = true;
		this.database = currentServer.getDatabase();
	}
	
	/**
	 * Initializes and starts the client connection. 
	 * Loops until the connection is closed or aborted by the client.
	 */
	public void run() {
		try {
			output = clientSocket.getOutputStream();
			input = clientSocket.getInputStream();
			
			while(isOpen) {
				try {
					KVMessage latestMsg = receiveMessage();
					if(latestMsg==null)
						break;
					processKVMessage(latestMsg);
				} catch (IOException ioe) {
					logger.error("Error! Connection lost!");
					isOpen = false;
				}				
			}
			
		} catch (IOException ioe) {
			logger.error("Error! Connection could not be established!", ioe);
			
		} finally {
			
			try {
				if (clientSocket != null) {
					input.close();
					output.close();
					clientSocket.close();
				}
			} catch (IOException ioe) {
				logger.error("Error! Unable to tear down connection!", ioe);
			}
		}
	}
	
	
	
	/**
	 * @param message
	 * 
	 * Receives a object KVMEssage and executes the method get or put, sending back the 
	 * response from the command
	 * 
	 */
	private void processKVMessage(KVMessage message){
		if(message.getStatus().equals(KVMessage.StatusType.GET)){
			KVMessage response = get(message.getKey());
			try {
				sendMessage(response);
			} catch (IOException e) {
				logger.error("error while sending response");
			}
		}
		else if (message.getStatus().equals(KVMessage.StatusType.PUT)){
			KVMessage response = put(message.getKey(),message.getValue());
			try {
				sendMessage(response);
			} catch (IOException e) {
				logger.error("erorr while sending response");
			}
		}
		else{
			logger.error("invalid request");
		}
		
	}
	
	/**
	 * Method sends a KVMessage using this socket.
	 * @param msg the message that is to be sent.
	 * @throws IOException some I/O error regarding the output stream 
	 * 
	 * To send the messages it sends directly its bytes
	 * 
	 */
	public void sendMessage(KVMessage msg) throws IOException {
		byte[] msgBytes = msg.getBytes();
		output.write(msgBytes, 0, msgBytes.length);
		output.flush();
		logger.info("SEND \t<" 
				+ clientSocket.getInetAddress().getHostAddress() + ":" 
				+ clientSocket.getPort() + ">: '" 
				+ msg.getStatus().name() +"'");
    }
	
	
	
	/**
	 * @return KVMessage response
	 * @throws IOException
	 * 
	 * This fucntion reads the bytes from the socket until it finds a newline character
	 * Then it passess the byte array to KVMessageImpl which will try to marshal it into
	 * a KVMessage, which will be returned by the method
	 */
	private KVMessage receiveMessage() throws IOException {
		
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
		KVMessage retvalue;
		try{
			retvalue = new KVMessageImpl(msgBytes);
		}
		catch(IllegalArgumentException e){
			logger.error(e.getMessage());
			retvalue = null;
		}
		
		return retvalue;
    }
	
	
    /**
     * @param key
     * @return KVMessage
     * Returns KVMessage.GET_ERROR if key not found, otherwise KVMessage with value
     */
    public KVMessage get(String key){
    	if (database.containsKey(key)){
    		return new KVMessageImpl(KVMessage.StatusType.GET_SUCCESS, (String)database.get(key));
    	}
    	else{
    		return new KVMessageImpl(KVMessage.StatusType.GET_ERROR);
    	}
    	
    }
    
    /**
     * @param key
     * @param value
     * @return KVMessage
     * this function puts a value into the server
     * it is synchronized to avoid write conflicts (the HashMap is also part of the synchronized
     * collection)
     * If value is "null" it will delete the element
     */
    synchronized public KVMessage put(String key, String value){
    	KVMessageImpl response;

		if (database.containsKey(key)){
	    	if(value.equals("null")){
	    		try{
	    			database.remove(key);
	    			logger.info("deleted:  " + database.get(key));
	    			response = new KVMessageImpl(KVMessage.StatusType.DELETE_SUCCESS);
	    		}
	    		catch(Exception e){
	    			response = new KVMessageImpl(KVMessage.StatusType.DELETE_ERROR);
	    		}
	    	}
	    	else{
    			try{
    				database.put(key, value);
    				logger.info("Received " + key + value);
    				response = new KVMessageImpl(KVMessage.StatusType.PUT_UPDATE, value);
    			}
    			catch(Exception e){
    				response = new KVMessageImpl(KVMessage.StatusType.PUT_ERROR);
    			}
	    	}
		}
		else{
			if(value.equals("null")){
				response = new KVMessageImpl(KVMessage.StatusType.DELETE_ERROR);
			}
			else{
				database.put(key, value);
				response = new KVMessageImpl(KVMessage.StatusType.PUT_SUCCESS);
			}
		}
    	
    	return response;

    }
	
}