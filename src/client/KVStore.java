package client;


import java.net.*;
import java.io.*;

import org.apache.log4j.Logger;

import common.messages.KVMessage;
import common.objects.Metadata;
import communication.CommunicationModule;

public class KVStore implements KVCommInterface {

    private Logger logger = Logger.getRootLogger();
    CommunicationModule connection;
    Metadata metadata;
    
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
    public KVMessage put(String key, String value) throws Exception {
            KVMessage request = new KVMessageImpl(KVMessage.StatusType.PUT, key, value);
            connection.sendBytes(request.getBytes());
            byte [] response = connection.receiveBytes();
            KVMessage retval = new KVMessageImpl(response);
            return retval;
    }

    /* (non-Javadoc)
     * @see client.KVCommInterface#get(java.lang.String)
     * 
     * Gets the message and returns the KVMessage response
     */
    @Override
    public KVMessage get(String key) throws Exception {
            KVMessage request = new KVMessageImpl(KVMessage.StatusType.GET, key);
            connection.sendBytes(request.getBytes());
            byte [] response = connection.receiveBytes();
            KVMessage retval = new KVMessageImpl(response);
            return retval;
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
