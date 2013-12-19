package client;


import java.net.*;
import java.io.*;

import org.apache.log4j.Logger;

import common.messages.KVMessage;
import common.messages.KVMessageImpl;
import common.messages.Message;
import common.objects.Metadata;
import common.objects.ServerInfo;
import common.utilis.Hash;
import communication.CommunicationModule;

/**
 * 
 * @author Dario
 * 
 * Library for the communication with a KVServer
 * It is transparent to the client, if the server is not responsible
 * it will connect to a new one and return the correct value
 *
 */

public class KVStore implements KVCommInterface {

    private Logger logger = Logger.getLogger(KVStore.class);
    private CommunicationModule connection;
    private Metadata metadata;


	public KVStore(String address, int port){
            this.connection = new CommunicationModule(address, port);
    }
    
    @Override
    public void connect() throws UnknownHostException, IOException {
            connection.connect();
    }        
    
    /* (non-Javadoc)
     * @see client.KVCommInterface#put(java.lang.String, java.lang.String)
     * 
     * Puts the message and returns the KVMessage Response
     * 
     */
    @Override
    public KVMessage put(String key, String value) throws IOException {
		byte[] kvmessage_payload = new KVMessageImpl(KVMessage.StatusType.PUT, key, value).getBytes();
        Message request = new Message(Message.PermissionType.USER, kvmessage_payload);
        connection.sendBytes(request.getBytes());
        byte [] response = connection.receiveBytes();
        KVMessage retmsg = new KVMessageImpl(response);
        
        // Messages which are printed back to the client
        if(retmsg.getStatus().equals(KVMessage.StatusType.PUT_SUCCESS) ||
        		retmsg.getStatus().equals(KVMessage.StatusType.PUT_ERROR) ||
        		retmsg.getStatus().equals(KVMessage.StatusType.PUT_UPDATE) ||
        		retmsg.getStatus().equals(KVMessage.StatusType.SERVER_STOPPED) ||
        		retmsg.getStatus().equals(KVMessage.StatusType.DELETE_SUCCESS) ||
        		retmsg.getStatus().equals(KVMessage.StatusType.DELETE_ERROR) ||
        		retmsg.getStatus().equals(KVMessage.StatusType.SERVER_WRITE_LOCK)){
        	
        	return retmsg;
        }
        
        // Server not responsible, we read the metadata received with the the message and
        // connect to the right one
        else if(retmsg.getStatus().equals(KVMessage.StatusType.SERVER_NOT_RESPONSIBLE)){
        	this.metadata = retmsg.getMetaData();
        	ServerInfo serverinfo = this.metadata.get(Hash.md5(key));
        	disconnect();
        	this.connection = new CommunicationModule(serverinfo.getAddress(), serverinfo.getPort());
        	connection.connect();
        	logger.debug("[PUT] server not responsible, connecting to " + serverinfo.getAddress() + ":" + serverinfo.getPort());
        	
        	return put(key, value);
        }
        else{
        	logger.error("Unexpected return message");
        	return new KVMessageImpl(KVMessage.StatusType.PUT_ERROR);
        }
    }

    /* (non-Javadoc)
     * @see client.KVCommInterface#get(java.lang.String)
     * 
     * Gets the message and returns the KVMessage response
     */
    @Override
    public KVMessage get(String key) throws IOException {
		byte[] kvmessage_payload = new KVMessageImpl(KVMessage.StatusType.GET, key).getBytes();
        Message request = new Message(Message.PermissionType.USER, kvmessage_payload);
        connection.sendBytes(request.getBytes());
        byte [] response = connection.receiveBytes();
        KVMessage retmsg = new KVMessageImpl(response);
        
        // Messages which are printed back to the client
        if(retmsg.getStatus().equals(KVMessage.StatusType.GET_SUCCESS) ||
        		retmsg.getStatus().equals(KVMessage.StatusType.GET_ERROR) ||
        		retmsg.getStatus().equals(KVMessage.StatusType.SERVER_STOPPED) ||
        		retmsg.getStatus().equals(KVMessage.StatusType.SERVER_WRITE_LOCK)){
        	
        	return retmsg;
        }
        
        // Read metadata and connect to the right server
        else if(retmsg.getStatus().equals(KVMessage.StatusType.SERVER_NOT_RESPONSIBLE)){
        	this.metadata = retmsg.getMetaData();
        	ServerInfo serverinfo = this.metadata.get(Hash.md5(key));
        	disconnect();
        	this.connection = new CommunicationModule(serverinfo.getAddress(), serverinfo.getPort());
        	connection.connect();
        	logger.debug("[GET] server not responsible, connecting to " + serverinfo.getAddress() + ":" + serverinfo.getPort());
        	
        	return get(key);
        	
        }
        else{
        	logger.error("Unexpected return message");
        	return new KVMessageImpl(KVMessage.StatusType.GET_ERROR);
        }
    }


    @Override
    public void disconnect() {
            try {
                    connection.closeConnection();
            } catch (IOException e) {
                    logger.error("Unable to close connection.");
            }
    }        

	
}
