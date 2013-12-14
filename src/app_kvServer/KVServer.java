package app_kvServer;

import java.io.IOException;
import java.net.*;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import logger.LogSetup;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import common.objects.Metadata;
import communication.CommunicationModule;

public class KVServer extends Thread  {
	
	private static Logger logger = Logger.getRootLogger();
	private int port;
    private ServerSocket serverSocket;
    private boolean running;
    private Map<String, String> database;
    
	/**
	 * Start KV Server at given port
	 * @param port given port for storage server to operate
	 */
	public KVServer(int port) {
		this.port = port;
		database = Collections.synchronizedMap(new HashMap<String,String>());
	}
	
    /**
     * Creates the ServerSocket for the server at the given port
     */
    private boolean initKVServer(Metadata meta) {
    	logger.info("Starting Server");
    	try {
            serverSocket = new ServerSocket(port);
            logger.info("Server listening on port: " 
            		+ serverSocket.getLocalPort());    
            return true;
        
        } catch (IOException e) {
        	logger.error("Error! Cannot open server socket:");
            if(e instanceof BindException){
            	logger.error("Port " + port + " is already bound!");
            }
            return false;
        }
    }
	
    private boolean isRunning() {
        return this.running;
    }
    
    /**
     * Starts the server
     */
    public void start() {
        
    	running = initKVServer(null);
        if(serverSocket != null) {
	        while(isRunning()){
	            try {
	            	
	                Socket client = serverSocket.accept();                
	               // CommunicationModule connection = 
	               // 		new CommunicationModule(client, this.database);
	               // new Thread(connection).start();
	                
	                logger.info("Connected to " 
	                		+ client.getInetAddress().getHostName() 
	                		+  " on port " + client.getPort());
	            } catch (IOException e) {
	            	logger.error("Error! " +
	            			"Unable to establish connection. \n", e);
	            }
	        }
        }
        logger.info("Server stopped.");
    }
    
    /**
     * Stops the server insofar that it won't listen at the given port any more.
     */
    public void stopS(){
        running = false;
        try {
			serverSocket.close();
		} catch (IOException e) {
			logger.error("Error! " +
					"Unable to close socket on port: " + port, e);
		}
    }
}
