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

/**
 * KVServer main class
 * It holds the database and the variables necessary for
 * interaction with the distributed system
 * @author ambi
 *
 */
public class KVServer extends Thread  {
	

	private int port;
	private Logger logger;
	private ServerSocket serverSocket;
	private boolean running;
    private Map<String, String> database;
    private boolean acceptingRequests;
    private boolean writeLock;
	private Metadata metadata;
    private String key;
    


	/**
	 * Start KV Server at given port
	 * @param port given port for storage server to operate
	 */
	public KVServer(int port) {
		this.port = port;
		logger = Logger.getLogger("KVServer at " + port);
		this.database = Collections.synchronizedMap(new HashMap<String,String>());
		this.acceptingRequests = false;
		this.writeLock = false;
		logger.info("Starting");
	}
	
    /**
     * Creates the ServerSocket for the server at the given port
     */
    private boolean initKVServer() {
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
    
    // GETTERS AND SETTERS
    // -------------------------
	
    public String getKey() {
		return key;
	}

	public void setKey(String key){
		this.key = key;
	}
	
    /**
	 * @return the metadata
	 */
	public Metadata getMetadata() {
		return metadata;
	}

	/**
	 * @param metadata the metadata to set
	 */
	public void setMetadata(Metadata metadata) {
		logger.debug("setting metadata: " + metadata.toString());
		this.metadata = metadata;
	}
	
    /**
	 * @return the serverSocket
	 */
	public ServerSocket getServerSocket() {
		return serverSocket;
	}

	private boolean isRunning() {
        return this.running;
    }
	
    public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}
	
    /**
	 * @return the writeLock
	 */
	public boolean isWriteLock() {
		return writeLock;
	}

	/**
	 * @param writeLock the writeLock to set
	 */
	public void setWriteLock(boolean writeLock) {
		this.writeLock = writeLock;
	}

    
    /**
     * Starts the server
     */
    public void start() {
        logger.info("KVServer starting at port " + port);
    	running = initKVServer();
        if(serverSocket != null) {
        	while(isRunning()){
	            try {
	                Socket client = serverSocket.accept();                
	                ClientConnection connection = 
	                		new ClientConnection(client, this);
	                new Thread(connection).start();
	                
	                logger.info("Connected to " 
	                		+ client.getInetAddress().getHostName() 
	                		+  " on port " + client.getPort());
	            } catch (IOException e) {
	            	logger.error("Shutting down");
	            }
	        }
        }
        logger.info("Server stopped.");
    }
    
    public void setacceptingRequests(boolean val){
    	this.acceptingRequests = val;
    }
    
    public boolean getacceptingRequests(){
    	return this.acceptingRequests;
    }
    
    /**
     * Stops the server insofar that it won't listen at the given port any more.
     */
    public void shutDown(){
        running = false;
        try {
			serverSocket.close();
		} catch (IOException e) {
			logger.error("Error! " +
					"Unable to close socket on port: " + port, e);
		}
        System.exit(0);
    }
    
	public Map<String, String> getDatabase() {
		return this.database;
	}
    
    /**
     * Main entry point for the echo server application. 
     * @param args contains the port number at args[0].
     */
    public static void main(String[] args) {
    	try {
			new LogSetup("logs/server/server.log", Level.ALL);
			if(args.length != 1) {
				System.out.println("Error! Invalid number of arguments!");
				System.out.println("Usage: Server <port>!");
			} else {
				int port = Integer.parseInt(args[0]);
				new KVServer(port).start();
			}
		} catch (IOException e) {
			System.out.println("Error! Unable to initialize logger!");
			e.printStackTrace();
			System.exit(1);
		} catch (NumberFormatException nfe) {
			System.out.println("Error! Invalid argument <port>! Not a number!");
			System.out.println("Usage: Server <port>!");
			System.exit(1);
		}
    }
}
