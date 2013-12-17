package app_kvEcs;

import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.log4j.*;
import common.messages.*;
import common.objects.ServerInfo;
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

	public ECSClientConnection(Socket clientSocket, List <ServerInfo> iParameters, List <ServerInfo> startedServers) {
		this.clientSocket = clientSocket;
		this.serverPool = iParameters;
		this.startedServers = startedServers;
		this.isOpen = true;
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
	
	public void initService(int numberOfNodes) {
		
		// Setting max length of servers if the client asks for a number too big
		if(numberOfNodes>serverPool.size())
			numberOfNodes=serverPool.size();
		
		startedServers = pickRandomElements(serverPool, numberOfNodes);
		for(ServerInfo i: startedServers) {
			ProcessBuilder pb = new ProcessBuilder("nohup", "ssh", "-n", i.getAddress(), "java -jar", "ms3-server.jar", ""+i.getPort(), "&");
			pb.redirectErrorStream();
			try {
				pb.start();
			} catch (IOException e) {
				logger.info("Unable to launch SSH process");
			}
	    }
	}

	public static List<ServerInfo> pickRandomElements(List <ServerInfo> array, int n) {
	    List<ServerInfo> list = new ArrayList<ServerInfo>(array.size());
	    for (ServerInfo i : array)
	        list.add(i);
	    Collections.shuffle(list);

	    List<ServerInfo> answer = new ArrayList<ServerInfo>(n);
	    for (int i = 0; i < n; i++)
	        answer.add(list.get(i));
	    return answer;
	}
	

	public void start() {
		logger.info("Received START command");
		for(ServerInfo i: this.startedServers){
			
		}
	}


	public void stop() {
		// TODO Auto-generated method stub
		
	}


	public void shutDown() {
		// TODO Auto-generated method stub
		
	}


	public void addNode() {
		// TODO Auto-generated method stub
		
	}


	public void removeNode() {
		// TODO Auto-generated method stub
		
	}

	private KVAdminMessage receiveMessage() throws IOException {
		
		byte[] msgBytes = connection.receiveBytes();
		KVAdminMessage retvalue;
		try{
			retvalue = new KVAdminMessageImpl(msgBytes);
		}
		catch(IllegalArgumentException e){
			logger.error(e.getMessage());
			retvalue = null;
		}
		
		return retvalue;
    }
	
}