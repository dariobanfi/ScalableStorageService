package app_kvEcs;

import java.io.*;
import java.net.*;
import common.messages.KVAdminMessage;
import common.messages.KVAdminMessageImpl;
import common.messages.Message;
import communication.CommunicationModule;


public class ECSCommunicationLibrary  {
	
	CommunicationModule connection;
	
	public ECSCommunicationLibrary(String server, int port) throws UnknownHostException, IOException{
		connection = new CommunicationModule(server,port);
		connection.connect();
	}


	public void initService(int numberOfNodes) throws IOException {
		KVAdminMessage admin_msg = new KVAdminMessageImpl(KVAdminMessage.StatusType.INIT_SERVICE, numberOfNodes);
		Message msg = new Message(Message.PermissionType.ADMIN, admin_msg.getBytes());
		connection.sendBytes(msg.getMsgBytes());
	}

	public void start() throws IOException {
		KVAdminMessage admin_msg = new KVAdminMessageImpl(KVAdminMessage.StatusType.START);
		Message msg = new Message(Message.PermissionType.ADMIN, admin_msg.getBytes());
		connection.sendBytes(msg.getMsgBytes());
	}


	public void stop() throws IOException {
		KVAdminMessage admin_msg = new KVAdminMessageImpl(KVAdminMessage.StatusType.STOP);
		Message msg = new Message(Message.PermissionType.ADMIN, admin_msg.getBytes());
		connection.sendBytes(msg.getMsgBytes());
	}


	public void shutDown() throws IOException {
		KVAdminMessage admin_msg = new KVAdminMessageImpl(KVAdminMessage.StatusType.SHUTDOWN);
		Message msg = new Message(Message.PermissionType.ADMIN, admin_msg.getBytes());
		connection.sendBytes(msg.getMsgBytes());		
	}


	public void addNode() throws IOException {
		KVAdminMessage admin_msg = new KVAdminMessageImpl(KVAdminMessage.StatusType.ADD_NODE);
		Message msg = new Message(Message.PermissionType.ADMIN, admin_msg.getBytes());
		connection.sendBytes(msg.getMsgBytes());		
	}


	public void removeNode() throws IOException {
		KVAdminMessage admin_msg = new KVAdminMessageImpl(KVAdminMessage.StatusType.REMOVE_NODE);
		Message msg = new Message(Message.PermissionType.ADMIN, admin_msg.getBytes());
		connection.sendBytes(msg.getMsgBytes());
	}
	
	public void disconnect() throws IOException{
		connection.closeConnection();
	}

}
