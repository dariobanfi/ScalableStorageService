package app_kvEcs;

import java.io.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import common.messages.KVAdminMessage;
import common.objects.Metadata;
import common.objects.Range;
import common.objects.ServerInfo;
import logger.LogSetup;

/**
 * 
 * @author Dario
 * 
 * ECSServer class, it provides a configuration service for
 * the underlying distributed system
 *
 */
public class ECSServer implements ECSClientInterface{
	
	public static Logger logger = Logger.getLogger(ECSServer.class);
	
	// Server parsed from settings.config and server launched
	List <ServerInfo> serverPool = new ArrayList<ServerInfo>();
 	List <ServerInfo> startedServers = new ArrayList<ServerInfo>();



	private Metadata metadata;
	private boolean init=false;

	public ECSServer(String config) {
		parseConfig(config);
	}
	
	/**
	 * Parses the config file passed as parameter
	 * @param location of the file
	 */
	public void parseConfig(String location){
		try {
			File file = new File(location);
			FileReader fileReader = new FileReader(file);
			BufferedReader bufferedReader = new BufferedReader(fileReader);
			String line;
			while ((line = bufferedReader.readLine()) != null) {
				String[] tokens = line.split("\\s+");
				ServerInfo temp = new ServerInfo(tokens[1], Integer.parseInt(tokens[2]));
				serverPool.add(temp);
			}
			fileReader.close();
			logger.debug("Parsed " + serverPool.size() + " servers");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public boolean initService(int numberOfNodes) {
		
		// Init check to see if we already initializied the ecs
		
		if(init){
			return false;
		}
		
		init = true;
		
		// Initializing metadata
		
		metadata = new Metadata();
		
		// Getting directory of jar files
		
		//String workdir = System.getProperty("user.dir");
		String workdir = "/tmp"; //Require for Alex Servers
		
		// Setting max length of servers if the client asks for a number too big
		if(numberOfNodes>serverPool.size())
			numberOfNodes=serverPool.size();
		
		// Launching SSH start for n servers chose with numberOfNodes
		startedServers = pickRandomElements(serverPool, numberOfNodes);
		for(ServerInfo i:startedServers)
			serverPool.remove(i);
		
		
		for(ServerInfo i: startedServers) {
			String script = "ssh -n " + i.getAddress() + " nohup java -jar " 
			+ workdir + "/ms3-server.jar " + i.getPort() + " &";
			
			logger.debug(script);
			Runtime run = Runtime.getRuntime();
			try {
			   run.exec(script);
				metadata.add(i);
				logger.info("Launched the SSH Processes for server " + i.toString());
			} catch (IOException e) {
				logger.info("Unable to launch SSH process");
			}
	    }
		
		//Waiting for 5 seconds to be sure servers are up and ready to answer
		
		logger.debug("Waiting for 5 seconds");
		try {
		    Thread.sleep(5000);
		} catch(InterruptedException ex) {
		    Thread.currentThread().interrupt();
		}
		
		// Initializing the servers who succesfully launched with
		// the metadata by calling initKVServer
		
		for(ServerInfo i: startedServers){
			try {
				ECSServerKVLibrary e = new ECSServerKVLibrary(i.getAddress(),i.getPort());
				String key = i.toHash();
				KVAdminMessage response = e.initKVServer(key, metadata);
				if(response==null){
					logger.error("Server did not respond, aborting");
				}
				else{
					logger.info(response.getStatusType());
					if(response.getStatusType().equals(KVAdminMessage.StatusType.SUCCESS)){
						logger.debug("Server succesfully initiated");
					}
					else{
						logger.error("Cannot initiate server with metadata");
					}
					e.disconnect();
				}
			} catch (IOException e) {
				logger.error("Unable to connect to " + i.toString());
			}		
		}
		
		return true;
		
	}
	
	/**
	 * 
	 * Creates a new server instance
	 * @param s, ServerInfo to start
	 */
	
	private void initSingleServer(ServerInfo s){
		
		// Getting directory of jar files
		
		String workdir = System.getProperty("user.dir");
		
		// SSH Call
		
		String script = "ssh -n " + s.getAddress() + " nohup java -jar " 
		+ workdir + "/ms3-server.jar " + s.getPort() + " &";
		
		logger.debug(script);
		Runtime run = Runtime.getRuntime();
		try {
		   run.exec(script);
			metadata.add(s);
			logger.info("Launched the SSH Processes for server " + s.toString());
		} catch (IOException e) {
			logger.info("Unable to launch SSH process");
		}
		
		// Waiting to be sure the ssh server is ready
		try {
		    Thread.sleep(3000);
		} catch(InterruptedException ex) {
		    Thread.currentThread().interrupt();
		}
		
		// Sending the server the metadata
		try {
			ECSServerKVLibrary e = new ECSServerKVLibrary(s.getAddress(),s.getPort());
			String key = s.toHash();
			KVAdminMessage response = e.initKVServer(key, metadata);
			logger.debug(response.getStatusType());
			if(response.getStatusType().equals(KVAdminMessage.StatusType.SUCCESS)){
				logger.info("Server succesfully initiated");
				
				// Starting server
				
				KVAdminMessage start = e.start();
				if(start.getStatusType().equals(KVAdminMessage.StatusType.SUCCESS))
					logger.debug("Server succesfully started");
				else
					logger.error("Cannot start server");
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
	 * @param array of elements to pick
	 * @param n, number of elements to pick
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
	    for (int i = 0; i < n; i++){
	        answer.add(list.get(i));
	    }
	    return answer;
	}
	
	/**
	 * Launches a START command for every server which already had
	 * been initialized
	 */

	public void start() {
		for(ServerInfo i: startedServers){
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
	 */

	public void stop() {
		for(ServerInfo i: startedServers){
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
		logger.info("Received SHUTDOWN command for " + startedServers.size() + " servers");
		
		for(int i=0; i<startedServers.size();i++){
			try {
				ECSServerKVLibrary e = 
						new ECSServerKVLibrary(startedServers.get(i).getAddress(),startedServers.get(i).getPort());
				e.shutDown();
				serverPool.add(startedServers.get(i));
				e.disconnect();
			} catch (IOException e) {
				logger.error("Unable to connect to " + startedServers.get(i).toString());
			}	
		}
		startedServers.clear();
		init = false;
	}


	/**
	 * Adds a random node to the startedServers and recalculates
	 * the data distribution across the system
	 */
	public boolean addNode() {
		
		// Picking up a non-available random server
		ServerInfo newServer;
		try{
			newServer = pickRandomElements(serverPool, 1).get(0);
			startedServers.add(newServer);
			serverPool.remove(newServer);
		}
		catch(IllegalArgumentException e){
			logger.error("No available servers to add");
			return false;
		}
		
		// Getting successor 
		ServerInfo successorServer = metadata.get(newServer.toHash());
		
		// Calculating new metadata for the server

		metadata.add(newServer);
		
		// Initializing the new Storage server
		
		initSingleServer(newServer);
		
		// Setting write lock on the successor
		
		try {
			
			// Locking write on the successor
			ECSServerKVLibrary kvconnection_successor = 
					new ECSServerKVLibrary(successorServer.getAddress(), successorServer.getPort());
			kvconnection_successor.lockWrite();

			// Moving the data affected in the successor to the new server
			// The lower bound of the hash range is calculated with the getPredecessor() function
			// The serverinfo is then hashed to get the hex key
			
			String newServerKey = newServer.toHash();
			
			Range range = new Range(metadata.getPredecessor(newServerKey).toHash() , newServerKey); 
			
			// Moving data
			KVAdminMessage response = kvconnection_successor.moveData(range, newServer);
			
			if(response.getStatusType().equals(KVAdminMessage.StatusType.SUCCESS)){
				
				// Updating metadata for all servers
				updateServersMetadata();
				
				//Disabling serverLock
				kvconnection_successor.unlockWrite();
				
				// Cleaning elements not under the successor responisbility
				kvconnection_successor.cleanup(range);
				
				return true;
				
			}
			
			else{
				logger.error("Unexpected message " + response.getStatusType());
				return false;
			}
			
		} catch (IOException e) {
			logger.error("Unable to connect to the successor server");
			return false;
		}
		
	}
	
	/**
	 * Removes a random node from the startedServers and
	 * recalculates the data distribution across the system
	 */


	public boolean removeNode() {
		
		// Checking if this is the last node, if yes we shut down
		// if no nodes anymore we return false
		
		if(startedServers.size()==1){
			shutDown();
			return true;
		}
		
		// Picking up a started server
		ServerInfo dropServer;
		try{
			dropServer = pickRandomElements(startedServers, 1).get(0);
			startedServers.remove(dropServer);
			serverPool.add(dropServer);
			logger.info("Prepairing to remove " + dropServer.toString());
		}
		catch(IllegalArgumentException e){
			logger.error("No available servers to remove");
			return false;
		}
		
		String dropServerKey = dropServer.toHash();
		String dropServerPredecessorKey = metadata.getPredecessor(dropServerKey).toHash();
		
		// Updating metadata & getting successor server
		
		metadata.remove(dropServer);
		
		
		ServerInfo successorServer = metadata.get(dropServerKey);
		
		
		try {
			// Locking write on the server to be dropped
			ECSServerKVLibrary kvconnection = new ECSServerKVLibrary(dropServer.getAddress(), dropServer.getPort());
			kvconnection.lockWrite();
			
			// Sending the successor the updated metadata with his responsibilities
			ECSServerKVLibrary kvconnection_successor = new ECSServerKVLibrary(successorServer.getAddress(), successorServer.getPort());
			KVAdminMessage response_successor =  kvconnection_successor.update(metadata);
			if(!response_successor.getStatusType().equals(KVAdminMessage.StatusType.SUCCESS))
				logger.error("Successor server did not receive metadata");
			
			kvconnection_successor.disconnect();
			
			// Moving data
			Range range = new Range(dropServerPredecessorKey, dropServerKey);
			KVAdminMessage response = kvconnection.moveData(range, successorServer);
			
			if(response.getStatusType().equals(KVAdminMessage.StatusType.SUCCESS)){
				// Updating metadata for all servers
				updateServersMetadata();
				
				//Shutting down the server
				kvconnection.shutDown();
				return true;
			}
			return false;
		} catch (IOException e) {
			logger.error("[removeNode] Unable to contact the server");
			return false;
		}
		

	}
	
	/**
	 * Updates the metadata for all the started servers
	 */
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
	
 	
	/**
	 * @return the serverPool
	 */
	public List<ServerInfo> getServerPool() {
		return serverPool;
	}

	/**
	 * @param serverPool the serverPool to set
	 */
	public void setServerPool(List<ServerInfo> serverPool) {
		this.serverPool = serverPool;
	}

	/**
	 * @return the startedServers
	 */
	public List<ServerInfo> getStartedServers() {
		return startedServers;
	}

	/**
	 * @param startedServers the startedServers to set
	 */
	public void setStartedServers(List<ServerInfo> startedServers) {
		this.startedServers = startedServers;
	}
	

	public static void main(String[] args) {
		try {
			new LogSetup("logs/ecs/server.log", Level.ALL);
		} catch (IOException e1) {
			logger.error("Could not initialize logger");
		}
		
		if(args.length != 1) {
			logger.error("Error, invalid number of arguments!");
		} else {
			ECSServer ecs = new ECSServer(args[0]);
			new ECSClient(ecs).run();
		}
		
    }

}
