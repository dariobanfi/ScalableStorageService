package app_kvEcs;

import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.log4j.*;

import common.messages.*;
import common.objects.Metadata;
import common.objects.Range;
import common.objects.ServerInfo;
import common.utilis.Hash;
import communication.CommunicationModule;


/**
 * This class implements Runnable and it represents the thread that gets launched every time
 * a new socket request comes to the server.
 * It receives the serverList and the runningServersList in order to do operations on it
 */

public class ECSClientConnection implements Runnable {

	private static Logger logger = Logger.getRootLogger();
	private List <ServerInfo> serverPool;
	private List <ServerInfo> startedServers;
	private boolean isOpen;
	private Socket clientSocket;
	private CommunicationModule connection;
	private Metadata metadata;

	public ECSClientConnection(Socket clientSocket, Metadata metadata, List <ServerInfo> iParameters, List <ServerInfo> startedServers) {
		this.clientSocket = clientSocket;
		this.serverPool = iParameters;
		this.startedServers = startedServers;
		this.isOpen = true;
		this.metadata = metadata;
	}

	public void run() {
		try {
			connection = new CommunicationModule(clientSocket);
			while(isOpen) {
				try {
				    KVAdminMessage latestMsg = receiveMessage();
				    if(latestMsg!=null)
						processMessage(latestMsg);
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

	private void processMessage(KVAdminMessage msg){
		logger.info(new String(msg.getBytes()));
		if(msg.getStatusType().equals(KVAdminMessage.StatusType.INIT_SERVICE)){
			initService(msg.getNumber());
		}
		else if(msg.getStatusType().equals(KVAdminMessage.StatusType.START)){
			start();
		}
		else if(msg.getStatusType().equals(KVAdminMessage.StatusType.STOP)){
			stop();
		}
		else if(msg.getStatusType().equals(KVAdminMessage.StatusType.SHUTDOWN)){
			shutDown();
		}
		else if(msg.getStatusType().equals(KVAdminMessage.StatusType.ADD_NODE)){
			addNode();
		}
		else if(msg.getStatusType().equals(KVAdminMessage.StatusType.REMOVE_NODE)){
			removeNode();
		}
		else{
			logger.error("Unknown message");
		}
		
	}
	
	/**
	 * Allocates a number of servers defined by numberOfNodes and
	 * sends them the metadata
	 * @param numberOfNodes
	 */
	public void initService(int numberOfNodes) {
		
		// Setting max length of servers if the client asks for a number too big
		if(numberOfNodes>serverPool.size())
			numberOfNodes=serverPool.size();
		
		// Launching SSH start for n servers chose with numberOfNodes
		startedServers = pickRandomElements(serverPool, numberOfNodes);
		for(ServerInfo i: startedServers) {
			ProcessBuilder pb = new ProcessBuilder("nohup", "ssh", "-n", i.getAddress(), "java -jar", "ms3-server.jar", ""+i.getPort(), "&");
			pb.redirectErrorStream();
			try {
				pb.start();
				metadata.add(i);
			} catch (IOException e) {
				logger.info("Unable to launch SSH process");
			}
	    }
		
		//Waiting for 1 second to be sure servers are up and ready to answer
		
		try {
		    Thread.sleep(1000);
		} catch(InterruptedException ex) {
		    Thread.currentThread().interrupt();
		}
		
		// Initializing the servers who succesfully launched with
		// the metadata
		for(ServerInfo i: startedServers){
			try {
				ECSServerKVLibrary e = new ECSServerKVLibrary(i.getAddress(),i.getPort());
				KVAdminMessage response = e.initKVServer(metadata);
				logger.info(response.getStatusType());
				if(response.getStatusType().equals(KVAdminMessage.StatusType.SUCCESS)){
					logger.info("Server succesfully initiated");
				}
				else{
					logger.error("Cannot initiate server with metadata");
				}
				e.disconnect();
			} catch (IOException e) {
				logger.error("Unable to connect to " + i.toString());
			}		
		}
		
	}
	
	/**
	 * 
	 * Creates a new server instance
	 * @param s
	 */
	
	public void initSingleServer(ServerInfo s){
		ProcessBuilder pb = new ProcessBuilder("nohup", "ssh", "-n", s.getAddress(), "java -jar", "ms3-server.jar", ""+ s.getPort(), "&");
		pb.redirectErrorStream();
		try {
			pb.start();
			metadata.add(s);
		} catch (IOException e) {
			logger.info("Unable to launch SSH process");
		}
		
		// Waiting to be sure the ssh server is ready
		try {
		    Thread.sleep(1000);
		} catch(InterruptedException ex) {
		    Thread.currentThread().interrupt();
		}
		
		// Sending the server the metadata
		try {
			ECSServerKVLibrary e = new ECSServerKVLibrary(s.getAddress(),s.getPort());
			KVAdminMessage response = e.initKVServer(metadata);
			logger.info(response.getStatusType());
			if(response.getStatusType().equals(KVAdminMessage.StatusType.SUCCESS)){
				logger.info("Server succesfully initiated");
			}
			else{
				logger.error("Cannot initiate server with metadata");
			}
			e.disconnect();
		} catch (IOException e) {
			logger.error("Unable to connect to " + s.toString());
		}	
	}
	
	/**
	 * 
	 * @param array
	 * @param n
	 * @return an Array with n elements chosen randomly from the first array
	 */

	public static List<ServerInfo> pickRandomElements(List <ServerInfo> array, int n) {
		if(array.size()<1){
			throw new IllegalArgumentException();
		}
	    List<ServerInfo> list = new ArrayList<ServerInfo>(array.size());
	    for (ServerInfo i : array)
	        list.add(i);
	    Collections.shuffle(list);

	    List<ServerInfo> answer = new ArrayList<ServerInfo>(n);
	    for (int i = 0; i < n; i++)
	        answer.add(list.remove(i));
	    return answer;
	}
	
	/**
	 * Launches a start command for every server which already had
	 * been initialized
	 */

	public void start() {
		logger.info("Received START command");
		for(ServerInfo i: this.startedServers){
			try {
				ECSServerKVLibrary e = new ECSServerKVLibrary(i.getAddress(),i.getPort());
				KVAdminMessage response = e.start();
				logger.info(response.getStatusType());
				logger.info("Started" + i.toString());
				e.disconnect();
			} catch (IOException e) {
				logger.error("Unable to connect to " + i.toString());
			}
			
		}
	}

	/**
	 * Launches a stop command for servers who were initialized and started
	 * 
	 */

	public void stop() {
		logger.info("Received STOP command");
		for(ServerInfo i: this.startedServers){
			try {
				ECSServerKVLibrary e = new ECSServerKVLibrary(i.getAddress(),i.getPort());
				KVAdminMessage response = e.stop();
				logger.info(response.getStatusType());
				logger.info("Stopped" + i.toString());
				e.disconnect();
			} catch (IOException e) {
				logger.error("Unable to connect to " + i.toString());
			}
			
		}
	}

	/**
	 * Shuts down all the server instances
	 */

	public void shutDown() {
		logger.info("Received SHUTDOWN command");
		for(int i=0; i<this.startedServers.size();i++){
			try {
				ECSServerKVLibrary e = 
						new ECSServerKVLibrary(startedServers.get(i).getAddress(),startedServers.get(i).getPort());
				KVAdminMessage response = e.shutDown();
				logger.info(response.getStatusType());
				logger.info("Shut down" + startedServers.get(i).toString());
				this.serverPool.add(this.startedServers.remove(i));
				e.disconnect();
			} catch (IOException e) {
				logger.error("Unable to connect to " + startedServers.get(i).toString());
			}
			
		}
		
	}


	public void addNode() {

		// Picking up a non-available random server
		ServerInfo newServer;
		try{
			newServer = pickRandomElements(serverPool, 1).get(0);
			startedServers.add(newServer);
		}
		catch(IllegalArgumentException e){
			logger.error("No available servers to add");
			return;
		}
		// Getting successor 
		ServerInfo successorServer = metadata.get(Hash.md5(newServer.toString()));
		
		// Calculating new metadata for the server

		metadata.add(newServer);
		
		// Initializing the new Storage server
		
		initSingleServer(newServer);
		
		// Setting write lock on the successor
		
		try {
			ECSServerKVLibrary kvconnection = new ECSServerKVLibrary(successorServer.getAddress(), successorServer.getPort());
			kvconnection.lockWrite();

			// Moving the data affected in the successor to the new server
			// The lower bound of the hash range is calculated with the getPredecessor() function
			// The serverinfo is then hashed to get the hex key
			String newServerKey = Hash.md5(newServer.toString());
			Range range = new Range(Hash.md5(metadata.getPredecessor(newServerKey).toString()), newServerKey); 
			
			// Moving data
			KVAdminMessage response = kvconnection.moveData(range, successorServer);
			
			if(response.getStatusType().equals(KVAdminMessage.StatusType.SUCCESS)){
				// Updating metadata for all servers
				updateServersMetadata();
				
				//Disabling serverLock
				kvconnection.unlockWrite();
				
				// Cleaning elements not under the successor responisbility
				
				kvconnection.cleanup(range);
				
			}
			
			else{
				logger.error("Unexpected message " + response.getStatusType());
			}
			
		} catch (IOException e) {
			logger.error("Unable to connect to the successor server");
		}
		
	}


	public void removeNode() {
		
		// Picking up a started server
		ServerInfo dropServer;
		try{
			dropServer = pickRandomElements(this.serverPool, 1).get(0);
			startedServers.remove(dropServer);
			logger.info("Prepairing to remove " + dropServer.toString());
		}
		catch(IllegalArgumentException e){
			logger.error("No available servers to remove");
			return;
		}
		
		String dropServerKey = Hash.md5(dropServer.toString());
		
		// Updating metadata & getting successor server
		
		metadata.remove(dropServer);
		
		
		ServerInfo successorServer = metadata.get(dropServerKey);
		
		// Setting the write lock
		
		try {
			ECSServerKVLibrary kvconnection = new ECSServerKVLibrary(dropServer.getAddress(), dropServer.getPort());
			kvconnection.lockWrite();
			kvconnection.update(metadata);
			
			// Moving data
			Range range = new Range(Hash.md5(metadata.getPredecessor(dropServerKey).toString()), dropServerKey);
			KVAdminMessage response = kvconnection.moveData(range, successorServer);
			
			if(response.getStatusType().equals(KVAdminMessage.StatusType.SUCCESS)){
				// Updating metadata for all servers
				updateServersMetadata();
				
				//Shutting down the server
				kvconnection.shutDown();
				
			}
		} catch (IOException e) {
			logger.error("[removeNode] Unable to contact the server");
		}

	}
	
	public void updateServersMetadata(){
		for(ServerInfo i: startedServers){
			try {
				ECSServerKVLibrary e = new ECSServerKVLibrary(i.getAddress(),i.getPort());
				e.update(metadata);
				logger.info("Sending metadata update to " + i.toString());
				e.disconnect();
			} catch (IOException e) {
				logger.error("[METADATA UPDATE] Unable to connect to " + i.toString());
			}
		}
	}

	private KVAdminMessage receiveMessage() throws IOException {
		
		byte[] msgBytes = connection.receiveBytes();
		KVAdminMessage retvalue;
		try{
			retvalue = new KVAdminMessageImpl(msgBytes);
			if(retvalue.getStatusType() == null)
				throw new IOException("Malformed message");
		}
		catch(IllegalArgumentException e){
			logger.error(e.getMessage());
			retvalue = null;
		}
		
		return retvalue;
    }
	
}