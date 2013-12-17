package app_kvEcs;

import java.io.*;
import java.net.*;

import common.messages.KVAdminMessage;
import common.messages.KVAdminMessageImpl;
import common.messages.Message;
import common.objects.Metadata;
import common.objects.Range;
import common.objects.ServerInfo;
import communication.CommunicationModule;


public class ECSServerKVLibrary  {
	
	CommunicationModule connection;
	
	public ECSServerKVLibrary(String server, int port) throws UnknownHostException, IOException{
		connection = new CommunicationModule(server,port);
		connection.connect();
	}
	
	public KVAdminMessage initKVServer(Metadata metadata) throws IOException {
		KVAdminMessage admin_msg = new KVAdminMessageImpl(KVAdminMessage.StatusType.INIT_KV_SERVER, metadata);
		Message msg = new Message(Message.PermissionType.ADMIN, admin_msg.getBytes());
		connection.sendBytes(msg.getBytes());
        byte [] response = connection.receiveBytes();
        KVAdminMessage retmsg = new KVAdminMessageImpl(response);
        return retmsg;
	}

	public KVAdminMessage start() throws IOException {
		KVAdminMessage admin_msg = new KVAdminMessageImpl(KVAdminMessage.StatusType.START);
		Message msg = new Message(Message.PermissionType.ADMIN, admin_msg.getBytes());
		connection.sendBytes(msg.getBytes());
        byte [] response = connection.receiveBytes();
        KVAdminMessage retmsg = new KVAdminMessageImpl(response);
        return retmsg;
	}


	public KVAdminMessage stop() throws IOException {
		KVAdminMessage admin_msg = new KVAdminMessageImpl(KVAdminMessage.StatusType.STOP);
		Message msg = new Message(Message.PermissionType.ADMIN, admin_msg.getBytes());
		connection.sendBytes(msg.getBytes());
        byte [] response = connection.receiveBytes();
        KVAdminMessage retmsg = new KVAdminMessageImpl(response);
        return retmsg;
	}


	public KVAdminMessage shutDown() throws IOException {
		KVAdminMessage admin_msg = new KVAdminMessageImpl(KVAdminMessage.StatusType.SHUTDOWN);
		Message msg = new Message(Message.PermissionType.ADMIN, admin_msg.getBytes());
		connection.sendBytes(msg.getBytes());		
        byte [] response = connection.receiveBytes();
        KVAdminMessage retmsg = new KVAdminMessageImpl(response);
        return retmsg;
	}


	public KVAdminMessage lockWrite() throws IOException {
		KVAdminMessage admin_msg = new KVAdminMessageImpl(KVAdminMessage.StatusType.LOCK_WRITE);
		Message msg = new Message(Message.PermissionType.ADMIN, admin_msg.getBytes());
		connection.sendBytes(msg.getBytes());
        byte [] response = connection.receiveBytes();
        KVAdminMessage retmsg = new KVAdminMessageImpl(response);
        return retmsg;
	}

	public KVAdminMessage unlockWrite() throws IOException {
		KVAdminMessage admin_msg = new KVAdminMessageImpl(KVAdminMessage.StatusType.UNLOCK_WRITE);
		Message msg = new Message(Message.PermissionType.ADMIN, admin_msg.getBytes());
		connection.sendBytes(msg.getBytes());
        byte [] response = connection.receiveBytes();
        KVAdminMessage retmsg = new KVAdminMessageImpl(response);
        return retmsg;
	}
	
	public KVAdminMessage moveData(Range range, ServerInfo server) throws IOException {
		KVAdminMessage admin_msg = new KVAdminMessageImpl(KVAdminMessage.StatusType.MOVE_DATA, range, server);
		Message msg = new Message(Message.PermissionType.ADMIN, admin_msg.getBytes());
		connection.sendBytes(msg.getBytes());
        byte [] response = connection.receiveBytes();
        KVAdminMessage retmsg = new KVAdminMessageImpl(response);
        return retmsg;
	}
	
	public KVAdminMessage update(Metadata metadata) throws IOException {
		KVAdminMessage admin_msg = new KVAdminMessageImpl(KVAdminMessage.StatusType.UPDATE, metadata);
		Message msg = new Message(Message.PermissionType.ADMIN, admin_msg.getBytes());
		connection.sendBytes(msg.getBytes());
        byte [] response = connection.receiveBytes();
        KVAdminMessage retmsg = new KVAdminMessageImpl(response);
        return retmsg;
	}
	
	public void disconnect() throws IOException{
		connection.closeConnection();
	}

}
