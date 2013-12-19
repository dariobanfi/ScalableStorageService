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

/**
 * Class which handles the communication between a ECSServer
 * and a KVServer
 * @author Dario
 *
 */


public class ECSServerKVLibrary  {
	
	CommunicationModule connection;
	
	public ECSServerKVLibrary(String server, int port) throws UnknownHostException, IOException{
		connection = new CommunicationModule(server,port);
		connection.connect();
	}
	
	/**
	 * Inits a KVServer with the metadata and his own key
	 * @param key
	 * @param metadata
	 * @return response
	 * @throws IOException
	 */
	
	public KVAdminMessage initKVServer(String key, Metadata metadata) throws IOException {
		KVAdminMessage admin_msg = new KVAdminMessageImpl(KVAdminMessage.StatusType.INIT_KV_SERVER, key, metadata);
		
		// We wrap the KVAdminMessage in a Message with permissiontype Admin, so that the
		// server can differentiate user and admin messages 
		
		Message msg = new Message(Message.PermissionType.ADMIN, admin_msg.getBytes());
		connection.sendBytes(msg.getBytes());
        byte [] response = connection.receiveBytes();
        KVAdminMessage retmsg = new KVAdminMessageImpl(response);
        return retmsg;
	}

	/**
	 * Starts a server for client connection
	 * @return response
	 * @throws IOException
	 */
	public KVAdminMessage start() throws IOException {
		KVAdminMessage admin_msg = new KVAdminMessageImpl(KVAdminMessage.StatusType.START);
		Message msg = new Message(Message.PermissionType.ADMIN, admin_msg.getBytes());
		connection.sendBytes(msg.getBytes());
        byte [] response = connection.receiveBytes();
        KVAdminMessage retmsg = new KVAdminMessageImpl(response);
        return retmsg;
	}

	/**
	 * Stops accepting client requests
	 * @return response
	 * @throws IOException
	 */
	public KVAdminMessage stop() throws IOException {
		KVAdminMessage admin_msg = new KVAdminMessageImpl(KVAdminMessage.StatusType.STOP);
		Message msg = new Message(Message.PermissionType.ADMIN, admin_msg.getBytes());
		connection.sendBytes(msg.getBytes());
        byte [] response = connection.receiveBytes();
        KVAdminMessage retmsg = new KVAdminMessageImpl(response);
        return retmsg;
	}

	/**
	 * Shuts down a server
	 * @return response
	 * @throws IOException
	 */

	public KVAdminMessage shutDown() throws IOException {
		KVAdminMessage admin_msg = new KVAdminMessageImpl(KVAdminMessage.StatusType.SHUTDOWN);
		Message msg = new Message(Message.PermissionType.ADMIN, admin_msg.getBytes());
		connection.sendBytes(msg.getBytes());		
        byte [] response = connection.receiveBytes();
        KVAdminMessage retmsg = new KVAdminMessageImpl(response);
        return retmsg;
	}

	/**
	 * Locks the write of the server
	 * @return response
	 * @throws IOException
	 */
	
	public KVAdminMessage lockWrite() throws IOException {
		KVAdminMessage admin_msg = new KVAdminMessageImpl(KVAdminMessage.StatusType.LOCK_WRITE);
		Message msg = new Message(Message.PermissionType.ADMIN, admin_msg.getBytes());
		connection.sendBytes(msg.getBytes());
        byte [] response = connection.receiveBytes();
        KVAdminMessage retmsg = new KVAdminMessageImpl(response);
        return retmsg;
	}

	/**
	 * Unlocks the write of the server
	 * @return response
	 * @throws IOException
	 */
	public KVAdminMessage unlockWrite() throws IOException {
		KVAdminMessage admin_msg = new KVAdminMessageImpl(KVAdminMessage.StatusType.UNLOCK_WRITE);
		Message msg = new Message(Message.PermissionType.ADMIN, admin_msg.getBytes());
		connection.sendBytes(msg.getBytes());
        byte [] response = connection.receiveBytes();
        KVAdminMessage retmsg = new KVAdminMessageImpl(response);
        return retmsg;
	}
	
	/**
	 * Moves the data in the "range" to the server "server"
	 * @param range
	 * @param server
	 * @return response
	 * @throws IOException
	 */
	public KVAdminMessage moveData(Range range, ServerInfo server) throws IOException {
		KVAdminMessage admin_msg = new KVAdminMessageImpl(KVAdminMessage.StatusType.MOVE_DATA, range, server);
		Message msg = new Message(Message.PermissionType.ADMIN, admin_msg.getBytes());
		connection.sendBytes(msg.getBytes());
        byte [] response = connection.receiveBytes();
        KVAdminMessage retmsg = new KVAdminMessageImpl(response);
        return retmsg;
	}
	
	/**
	 * Updates the metadata of a server
	 * @param metadata
	 * @return response 
	 * @throws IOException
	 */
	public KVAdminMessage update(Metadata metadata) throws IOException {
		KVAdminMessage admin_msg = new KVAdminMessageImpl(KVAdminMessage.StatusType.UPDATE, metadata);
		Message msg = new Message(Message.PermissionType.ADMIN, admin_msg.getBytes());
		connection.sendBytes(msg.getBytes());
        byte [] response = connection.receiveBytes();
        KVAdminMessage retmsg = new KVAdminMessageImpl(response);
        return retmsg;
	}
	
	/**
	 * Removes the elements stored in the KVServer's database in the range "range"
	 * @param range
	 * @return
	 * @throws IOException
	 */
	public KVAdminMessage cleanup(Range range) throws IOException {
		KVAdminMessage admin_msg = new KVAdminMessageImpl(KVAdminMessage.StatusType.CLEANUP, range);
		Message msg = new Message(Message.PermissionType.ADMIN, admin_msg.getBytes());
		connection.sendBytes(msg.getBytes());
        byte [] response = connection.receiveBytes();
        KVAdminMessage retmsg = new KVAdminMessageImpl(response);
        return retmsg;
	}
	
	/**
	 * Disconnects from the server
	 * @throws IOException
	 */
	public void disconnect() throws IOException{
		connection.closeConnection();
	}

}
