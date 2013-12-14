package app_kvEcs;

import java.io.*;
import java.net.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import common.messages.KVAdminMessage;
import common.messages.KVAdminMessageImpl;
import common.messages.KVMessage;
import common.messages.Message;
import common.objects.ServerInfo;
import communication.CommunicationModule;
import logger.LogSetup;


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

	public void start() {
		// TODO Auto-generated method stub
		
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
	
	public void disconnect() throws IOException{
		connection.closeConnection();
	}

}
