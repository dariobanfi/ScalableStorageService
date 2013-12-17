package app_kvEcs;

import java.io.*;
import java.net.*;
import common.messages.KVAdminMessage;
import common.messages.KVAdminMessageImpl;
import common.messages.Message;
import communication.CommunicationModule;


public class ECSClientServerLibrary  {
	
	CommunicationModule connection;
	
	public ECSClientServerLibrary(String server, int port) throws UnknownHostException, IOException{
		connection = new CommunicationModule(server,port);
		connection.connect();
	}


	public void initService(int numberOfNodes) throws IOException {
		KVAdminMessage admin_msg = new KVAdminMessageImpl(KVAdminMessage.StatusType.INIT_SERVICE, numberOfNodes);
		connection.sendBytes(admin_msg.getBytes());
	}

	public void start() throws IOException {
		KVAdminMessage admin_msg = new KVAdminMessageImpl(KVAdminMessage.StatusType.START);
		connection.sendBytes(admin_msg.getBytes());
	}


	public void stop() throws IOException {
		KVAdminMessage admin_msg = new KVAdminMessageImpl(KVAdminMessage.StatusType.STOP);
		connection.sendBytes(admin_msg.getBytes());
	}


	public void shutDown() throws IOException {
		KVAdminMessage admin_msg = new KVAdminMessageImpl(KVAdminMessage.StatusType.SHUTDOWN);
		connection.sendBytes(admin_msg.getBytes());		
	}


	public void addNode() throws IOException {
		KVAdminMessage admin_msg = new KVAdminMessageImpl(KVAdminMessage.StatusType.ADD_NODE);
		connection.sendBytes(admin_msg.getBytes());		
	}

	public void removeNode() throws IOException {
		KVAdminMessage admin_msg = new KVAdminMessageImpl(KVAdminMessage.StatusType.REMOVE_NODE);
		connection.sendBytes(admin_msg.getBytes());
	}
	
	public void disconnect() throws IOException{
		connection.closeConnection();
	}

}
