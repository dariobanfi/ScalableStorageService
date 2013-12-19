package app_kvServer;

import java.io.IOException;
import java.net.Socket;
import java.util.Iterator;
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
 * It handles the operation requested by the client and the ecs for the KVServer
 */
public class ClientConnection implements Runnable {

	private static Logger logger = Logger.getLogger(ClientConnection.class);
	
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
				    	
				    	// We check if we have to handle a user message or admin message
				    	
					    if(latestMsg.getPermission().equals(Message.PermissionType.ADMIN)){
					    	processKVAdminMessage(latestMsg);
					    } else if(latestMsg.getPermission().equals(Message.PermissionType.USER)){
					    	processKVMessage(latestMsg);
					    }
				    }
				    else
				    	break;
				} catch (IOException ioe) {
					logger.error("Error! Connection lost!" + ioe.getMessage());
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
			logger.debug("Received START message");
			start();
		}

		else if(msg.getStatusType().equals(KVAdminMessage.StatusType.STOP)){
			stop();
		}
		else if(msg.getStatusType().equals(KVAdminMessage.StatusType.INIT_KV_SERVER)){
			initKVServer(msg.getKey(), msg.getMetadata());
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
		logger.debug(new String(message.getBytes()));
		
		KVMessage msg = new KVMessageImpl(message.getPayload());
		KVMessage response;
		
		/*
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
		
		/*
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
	 * Method sends a KVMessage using his socket.
	 * @param msg the message that is to be sent.
	 * @throws IOException some I/O error regarding the output stream 
	 * 
	 * To send the messages it sends directly its bytes
	 * 
	 */
	public void sendMessage(KVMessage msg) throws IOException {
		byte[] msgBytes = msg.getBytes();
		connection.sendBytes(msgBytes);
		logger.debug("Sending back " + msg.getStatus());
    }
	
	public void sendMessage(KVAdminMessage msg) throws IOException {
		byte[] msgBytes = msg.getBytes();
		connection.sendBytes(msgBytes);
		logger.debug("Sending back " + msg.getStatusType());
    }
	
	/**
	 * @return KVMessage response
	 * @throws IOException
	 * 
	 * This function reads the bytes from the socket until it finds a newline character
	 * Then it passes the byte array to Message which will try to marshal it into
	 * a Message, which will be returned by the method
	 */
	private Message receiveMessage() throws IOException {
		
		byte[] msgBytes = connection.receiveBytes();
		Message retvalue;
		try{			
			retvalue = new Message(msgBytes);
			if(retvalue.getPermission() == null || retvalue.getPayload()==null)
				throw new IllegalArgumentException("Malformed message");
		}
		catch(IllegalArgumentException e){
			retvalue = null;
		}
		
		return retvalue;
    }
	
	/**
	 * Sends a positive acknowledgment for the receive command (SUCCESS)
	 */
	
	private void sendAck(){
		try {
			logger.debug("Sending ack");
			connection.sendBytes(new KVAdminMessageImpl(KVAdminMessage.StatusType.SUCCESS).getBytes());
		} catch (IOException e) {
			logger.error("Unable to send ACK back");
		}
	}
	
	
	// ADMIN OPERATIONS, CALLED BY ECS
	// ---------------------------------------------------
	
	// Initializing KVServer with metadata and the key and sends back a acknowledgement
	public void initKVServer(String key, Metadata metadata){
		logger.debug("Initializing server");
		server.setMetadata(metadata);
		server.setKey(key);
		sendAck();
	}
	
	// Starting and sending back a acknowledgment
	public void start(){
		logger.debug("Starting accepting clients");
		server.setacceptingRequests(true);
		sendAck();
	}
	
	// Stopping server and sending back a acknowledgment
	public void stop(){
		logger.debug("Stop accepting clients");
		server.setacceptingRequests(false);
		sendAck();
	}
	
	// Shuttding down server and sending back a acknowledgment
	public void shutDown(){
		logger.debug("Shutting down");
		sendAck();
		server.shutDown();

	}
	
	// Locking write on server and sending back a acknowldgment
	public void lockWrite(){
		logger.debug("Locking the write operations");
		server.setWriteLock(true);
		sendAck();
	}
	
	// Unlocking write on server and sending back a acknowledgment
	public void unlockWrite(){
		logger.debug("Unlocking the write operations");
		server.setWriteLock(false);
		sendAck();
	}
	
	/**
	 * Moving data on server and sending back a acknowledgment
	 * @param range
	 * @param server
	 */
	public void moveData(Range range, ServerInfo server){
		logger.debug("Moving elements to " + server.toString());
		Range range1=null;
		Range range2=null;
		
		// Since the hashing is circular, but String comparison is not, we split the 
		// ranges in two if the lower limit is higher than the upper one
		
		if(range.getLower_limit().compareTo(range.getUpper_limit())>0){
			range1 = new Range(range.getLower_limit(), "ffffffffffffffffffffffffffffffff");
			range2 = new Range("00000000000000000000000000000000", range.getUpper_limit());
			logger.debug("Moving from " + range1.getLower_limit() + " to " + range1.getUpper_limit());
			logger.debug("Moving from " + range2.getLower_limit() + " to " + range2.getUpper_limit());
		}
		else{
			range1 = new Range(range.getLower_limit(),range.getUpper_limit());
			logger.debug("Moving from " + range1.getLower_limit() + " to " + range1.getUpper_limit());
		}
			
		
		for (Map.Entry<String, String> entry : this.server.getDatabase().entrySet()) {
			String hashkey = Hash.md5(entry.getKey());
			if(hashkey.compareTo(range1.getLower_limit())>0 && hashkey.compareTo(range1.getUpper_limit())<=0 ){
				KVStore kv = new KVStore(server.getAddress(), server.getPort());
				try {
					kv.connect();
					logger.debug("Putting " + entry.getKey() + " " + entry.getValue() + " to " + server.toString());
					kv.put(entry.getValue(), entry.getValue());
				} catch (IOException e) {
					logger.error("[moveData] problem connecting to the server " + server.toString() );
				}
			}
		}
		
		
		// If we had splitted the ranges
		
		if(range2!=null){
			for (Map.Entry<String, String> entry : this.server.getDatabase().entrySet()) {
				String hashkey = Hash.md5(entry.getKey());
				if(hashkey.compareTo(range2.getLower_limit())>0 && hashkey.compareTo(range2.getUpper_limit())<=0 ){
					KVStore kv = new KVStore(server.getAddress(), server.getPort());
					try {
						kv.connect();
						logger.debug("Putting " + entry.getKey() + " " + entry.getValue() + " to " + server.toString());
						kv.put(entry.getValue(), entry.getValue());
					} catch (IOException e) {
						logger.error("[moveData] problem connecting to the server " + server.toString() );
					}
				}
			}		
		}
		
		sendAck();
		
	}
	
	/**
	 * Updating metadata and sending back a acknowledgment
	 * @param metadata
	 */
	public void update(Metadata metadata){
		logger.debug("Updating metadata");
		server.setMetadata(metadata);
		sendAck();
	}
	
	/**
	 * Cleaning up elements not under responsibility and sending back a acknowledgment
	 * @param range of the elements to clean
	 */
	public void cleanup(Range range){
		Range range1=null;
		Range range2=null;
		
		// Since the hashing is circular, but String comparaison is not, we split the 
		// ranges in two if the lowerlimit is higher than the upper one
		
		if(range.getLower_limit().compareTo(range.getUpper_limit())>0){
			range1 = new Range(range.getLower_limit(), "ffffffffffffffffffffffffffffffff");
			range2 = new Range("00000000000000000000000000000000", range.getUpper_limit());
			logger.debug("Removing from " + range1.getLower_limit() + " to " + range1.getUpper_limit());
			logger.debug("Removing from " + range2.getLower_limit() + " to " + range2.getUpper_limit());
		}
		else{
			range1 = new Range(range.getLower_limit(),range.getUpper_limit());
			logger.debug("Removing from " + range1.getLower_limit() + " to " + range1.getUpper_limit());
		}
		
		   for(Iterator<Map.Entry<String, String>> it = server.getDatabase().entrySet().iterator(); it.hasNext(); ) {
			      Map.Entry<String, String> entry = it.next();
			      String hashkey = Hash.md5(entry.getKey());
				   if(hashkey.compareTo(range1.getLower_limit())>0 && hashkey.compareTo(range1.getUpper_limit())<=0 ){
			        it.remove();
			        logger.debug("Cleaning " + entry.getKey() + " " + hashkey);
			      }
		   }

		if(range2!=null){
			
			   for(Iterator<Map.Entry<String, String>> it = server.getDatabase().entrySet().iterator(); it.hasNext(); ) {
				      Map.Entry<String, String> entry = it.next();
				      String hashkey = Hash.md5(entry.getKey());
					   if(hashkey.compareTo(range2.getLower_limit())>0 && hashkey.compareTo(range2.getUpper_limit())<=0 ){
				        it.remove();
				        logger.debug("Cleaning " + entry.getKey() + " " + hashkey);
				      }
			   }

			
		}
		
		sendAck();
	}
	
	
	
	// USER OPERATIONS, CALLED BY KVCLIENT
	// ---------------------------------------------------
	
	
    /**
     * @param key
     * @return KVMessage
     * Returns KVMessage.GET_ERROR if key not found, otherwise KVMessage with value
     */
    public KVMessage get(String key){
    	
    	/*
    	 * Determining if the key is in the server's range,
    	 * if not, we send the updated metadata back to the
    	 * client with the message SERVER_NOT_RESPONSIBLE
    	 */
    	
    	String hashedkey = Hash.md5(key);
    	
    	ServerInfo responsible_server = server.getMetadata().get(hashedkey);
    	
    	logger.debug("Responsible server: " + responsible_server.toHash());
    	logger.debug("This server: " + server.getKey());
    	
    	if(!responsible_server.toHash().equals(server.getKey())){
    		return new KVMessageImpl(KVMessage.StatusType.SERVER_NOT_RESPONSIBLE, server.getMetadata());
    	}
    	
    	
    	/*
    	 * The server is responsible, so we get its key
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
    	
    	/*
    	 * Determining if the key is in the server's range,
    	 * if not, we send the updated metadata back to the
    	 * client with the message SERVER_NOT_RESPONSIBLE
    	 */
    	
    	String hashedkey = Hash.md5(key);
    	ServerInfo responsible_server = server.getMetadata().get(hashedkey);
    	if(!responsible_server.toHash().equals(server.getKey())){

    		response = new KVMessageImpl(KVMessage.StatusType.SERVER_NOT_RESPONSIBLE, server.getMetadata());
    		return response;
    	}
    	
    	/*
    	 * If we are here, it means the server was responsible for
    	 * the hashkey, so we first check if he his not in WRITE_LOCK 
    	 * status
    	 */
    	logger.debug("Key"+key+"Writelock:"+server.isWriteLock()+"ContainsKey"+database.containsKey(key)
    			+"Value"+value);
    	
    	// Checking if the server is not locked
    	if(!server.isWriteLock()){
    		
			if (database.containsKey(key)){
		    	if(value.equals("null")){
		    		try{
		    			database.remove(key);
		    			logger.info("deleted:  " + database.get(key));
		    			return new KVMessageImpl(KVMessage.StatusType.DELETE_SUCCESS);
		    		}
		    		catch(Exception e){
		    			return new KVMessageImpl(KVMessage.StatusType.DELETE_ERROR);
		    		}
		    	}
		    	else{
    				database.put(key, value);
    				logger.info("Received " + key + value);
    				return new KVMessageImpl(KVMessage.StatusType.PUT_UPDATE, value);

		    	}
			}
			else{
				if(value.equals("null")){
					return new KVMessageImpl(KVMessage.StatusType.DELETE_ERROR);
				}
				else{
					database.put(key, value);
					return new KVMessageImpl(KVMessage.StatusType.PUT_SUCCESS);
				}
			}
    	}
    	
    	else{
    		return new KVMessageImpl(KVMessage.StatusType.SERVER_WRITE_LOCK);
    	}
    	
    	
    	
    }
	
}