package app_kvServer;

import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Map;

import org.apache.log4j.*;

import client.KVStore;
import common.messages.*;
import common.objects.Metadata;
import common.objects.Range;
import common.objects.ServerInfo;
import common.utilis.Hash;
import communication.CommunicationModule;


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
	private CommunicationModule connection;
	private Socket clientSocket;
	private KVServer server;
	
	/**
	 * Constructs a new CientConnection object for a given TCP socket.
	 * @param clientSocket the Socket object for the client connection.
	 */
	public ClientConnection(Socket clientSocket, KVServer server) {
		this.clientSocket = clientSocket;
		this.isOpen = true;
		this.server = server;
	}
	
	/**
	 * Initializes and starts the client connection. 
	 * Loops until the connection is closed or aborted by the client.
	 */
	public void run() {
		try {
			connection = new CommunicationModule(clientSocket);
			
			while(isOpen) {
				try {
				    Message latestMsg = receiveMessage();
				    if(latestMsg!=null){
					    if(latestMsg.getPermission().equals(Message.PermissionType.ADMIN)){
					    	processKVAdminMessage(latestMsg);
					    } else if(latestMsg.getPermission().equals(Message.PermissionType.USER)){
					    	processKVMessage(latestMsg);
					    }
				    }
				} catch (IOException ioe) {
					logger.error("Error! Connection lost!");
					isOpen = false;
				}				
			}
			
		} catch (IOException ioe) {
			logger.error("Error! Connection could not be established!", ioe);
			
		} finally {
			
			try {
				if (connection != null) {
					connection.closeConnection();
				}
			} catch (IOException ioe) {
				logger.error("Error! Unable to tear down connection!", ioe);
			}
		}
	}
	
	/**
	 * @param message
	 * 
	 * Receives a object a Message with ADMIN permission, and calls
	 * the handling function for the StatusType
	 * 
	 */
	private void processKVAdminMessage(Message message){
		KVAdminMessage msg = new KVAdminMessageImpl(message.getPayload());
		if(msg.getStatusType().equals(KVAdminMessage.StatusType.START)){
			start();
		}

		else if(msg.getStatusType().equals(KVAdminMessage.StatusType.STOP)){
			stop();
		}
		else if(msg.getStatusType().equals(KVAdminMessage.StatusType.INIT_KV_SERVER)){
			initKVServer(msg.getMetadata());
		}
		else if(msg.getStatusType().equals(KVAdminMessage.StatusType.SHUTDOWN)){
			shutDown();
		}
		else if(msg.getStatusType().equals(KVAdminMessage.StatusType.LOCK_WRITE)){
			lockWrite();
		}
		else if(msg.getStatusType().equals(KVAdminMessage.StatusType.UNLOCK_WRITE)){
			unlockWrite();
		}
		else if(msg.getStatusType().equals(KVAdminMessage.StatusType.MOVE_DATA)){
			moveData(msg.getRange(), msg.getServerInfo());
		}
		else if(msg.getStatusType().equals(KVAdminMessage.StatusType.UPDATE)){
			update(msg.getMetadata());
		}
		else if(msg.getStatusType().equals(KVAdminMessage.StatusType.CLEANUP)){
			cleanup(msg.getRange());
		}
		else{
			logger.error("Unknown admin message received!");
		}
	}
	
	
	/**
	 * @param message
	 * 
	 * Receives a object KVMEssage and executes the method get or put, sending back the 
	 * response from the command
	 * 
	 */
	private void processKVMessage(Message message){
		KVMessage msg = new KVMessageImpl(message.getPayload());
		KVMessage response;
		/**
		 * Checking if the server is accepting client
		 * requests, if not returning SERVER_STOPPED
		 */
		
		if(!server.getacceptingRequests()){
			response = new KVMessageImpl(KVMessage.StatusType.SERVER_STOPPED);
			try {
				sendMessage(response);
			} catch (IOException e) {
				logger.error("unable to send response");
			}
		}
		
		/**
		 * Server accepting clients, we process the 
		 * request
		 */
		else{
			
			if(msg.getStatus().equals(KVMessage.StatusType.GET)){
				response = get(msg.getKey());
				try {
					sendMessage(response);
				} catch (IOException e) {
					logger.error("unable to send response");
				}
			}
			else if (msg.getStatus().equals(KVMessage.StatusType.PUT)){
				response = put(msg.getKey(),msg.getValue());
				try {
					sendMessage(response);
				} catch (IOException e) {
					logger.error("unable to send response");
				}
			}
			else{
				logger.error("invalid request");
			}
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
		connection.sendBytes(msgBytes);
		logger.info("SEND \t<" 
				+ connection.toString());
    }
	
	public void sendMessage(KVAdminMessage msg) throws IOException {
		byte[] msgBytes = msg.getBytes();
		connection.sendBytes(msgBytes);
		logger.info("SEND \t<" 
				+ connection.toString());
    }
	
	/**
	 * @return KVMessage response
	 * @throws IOException
	 * 
	 * This function reads the bytes from the socket until it finds a newline character
	 * Then it passes the byte array to KVMessageImpl which will try to marshal it into
	 * a KVMessage, which will be returned by the method
	 */
	private Message receiveMessage() throws IOException {
		
		byte[] msgBytes = connection.receiveBytes();
		Message retvalue;
		try{
			retvalue = new Message(msgBytes);
			if(retvalue.getPermission() == null)
				throw new IOException("Malformed message");
		}
		catch(IllegalArgumentException e){
			logger.error(e.getMessage());
			retvalue = null;
		}
		
		return retvalue;
    }
	
	
	// ADMIN OPERATIONS, CALLED BY ECS
	// ---------------------------------------------------
	
	public void initKVServer(Metadata metadata){
		server.setMetadata(metadata);
	}
	
	public void start(){
		server.setacceptingRequests(true);
	}
	
	public void stop(){
		server.setacceptingRequests(false);
	}
	
	public void shutDown(){
		server.shutDown();
	}
	
	public void lockWrite(){
		server.setWriteLock(true);
	}
	
	public void unlockWrite(){
		server.setWriteLock(false);
	}
	
	public void moveData(Range range, ServerInfo server){
		for (Map.Entry<String, String> entry : this.server.getDatabase().entrySet()) {
			String hashkey = Hash.md5(entry.getKey());
			if(hashkey.compareTo(range.getLower_limit())>0 && hashkey.compareTo(range.getUpper_limit())<=0 ){
				
				KVStore kv = new KVStore(server.getAddress(), server.getPort());
				try {
					kv.connect();
					kv.put(entry.getValue(), entry.getValue());
				} catch (IOException e) {
					logger.info("[moveData] problem connecting to the server " + server.toString());
				}
				logger.debug("Moving " + entry.getKey() + " " + hashkey);
			}
		}
		
	}
	
	public void update(Metadata metadata){
		server.setMetadata(metadata);
	}
	
	public void cleanup(Range range){
		for (Map.Entry<String, String> entry : this.server.getDatabase().entrySet()) {
			String hashkey = Hash.md5(entry.getKey());
			if(hashkey.compareTo(range.getLower_limit())>0 && hashkey.compareTo(range.getUpper_limit())<=0 ){
				server.getDatabase().remove(entry.getKey());
				logger.debug("Clearning " + entry.getKey() + " " + hashkey);
			}
		}
	}
	
	
	
	
	
	
	// USER OPERATIONS, CALLED BY KVCLIENT
	// ---------------------------------------------------
	
	
    /**
     * @param key
     * @return KVMessage
     * Returns KVMessage.GET_ERROR if key not found, otherwise KVMessage with value
     */
    public KVMessage get(String key){
    	
    	/** 
    	 * Determining if the key is in the server's range,
    	 * if not, we send the updated metadata back to the
    	 * client with the message SERVER_NOT_RESPONSIBLE
    	 */
    	
    	String hashedkey = Hash.md5(key);
    	ServerInfo responsible_server = server.getMetadata().get(hashedkey);
    	if(!responsible_server.getAddress().equals(server.getServerSocket().getInetAddress().toString()) || 
    			!(responsible_server.getPort() == server.getServerSocket().getLocalPort())){

    		return new KVMessageImpl(KVMessage.StatusType.SERVER_NOT_RESPONSIBLE, server.getMetadata());
    	}
    	
    	/**
    	 * The server is responsbile, so we get its key
    	 */
    	if (server.getDatabase().containsKey(key)){
    		return new KVMessageImpl(KVMessage.StatusType.GET_SUCCESS, (String)server.getDatabase().get(key));
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
    	Map<String,String> database = server.getDatabase();
    	
    	/** 
    	 * Determining if the key is in the server's range,
    	 * if not, we send the updated metadata back to the
    	 * client with the message SERVER_NOT_RESPONSIBLE
    	 */
    	
    	String hashedkey = Hash.md5(key);
    	ServerInfo responsible_server = server.getMetadata().get(hashedkey);
    	if(!responsible_server.getAddress().equals(server.getServerSocket().getInetAddress().toString()) || 
    			!(responsible_server.getPort() == server.getServerSocket().getLocalPort())){

    		response = new KVMessageImpl(KVMessage.StatusType.SERVER_NOT_RESPONSIBLE, server.getMetadata());
    		return response;
    	}
    	
    	
    	/**
    	 * If we are here, it means the server was responsible for
    	 * the hashkey, so we first check if he his not in WRITE_LOCK 
    	 * status
    	 */
    	
    	if(!server.isWriteLock()){
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
    	}
    	
    	else{
    		response = new KVMessageImpl(KVMessage.StatusType.SERVER_WRITE_LOCK);
    	}
    	
    	return response;

    }
	
}