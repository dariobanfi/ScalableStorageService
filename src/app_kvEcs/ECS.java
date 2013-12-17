package app_kvEcs;

import java.io.*;
import java.net.BindException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import common.objects.Metadata;
import common.objects.ServerInfo;
import logger.LogSetup;


public class ECS extends Thread{
	static List <ServerInfo> serverPool = new ArrayList<ServerInfo>();
	static List <ServerInfo> startedServers = new ArrayList<ServerInfo>();
	private Metadata metadata;
	private int port;
	private ServerSocket serverSocket;
	private boolean running;
	private static Logger logger = Logger.getRootLogger();
	
	
    public ECS(int port) {
        this.port = port;
    }

	public void run() {
		running = initializeServer();
		if(serverSocket != null) {
		        while(isRunning()){
		            try {     
		                Socket client = serverSocket.accept();                
		                ECSClientConnection connection = 
		                        new ECSClientConnection(client, metadata, serverPool, startedServers);
		                new Thread(connection).start();
		                
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
	
	private boolean isRunning() {
		return this.running;
	}
	
	public void stopServer(){
		running = false;
		try {
			serverSocket.close();
	    } catch (IOException e) {
	    	logger.error("Error! " +
	    		"Unable to close socket on port: " + port, e);
	    	}
	}
	
    private boolean initializeServer() {
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
	
	public static void parseConfig(String location){
		try {
			File file = new File(location);
			FileReader fileReader = new FileReader(file);
			BufferedReader bufferedReader = new BufferedReader(fileReader);
			String line;
			while ((line = bufferedReader.readLine()) != null) {
				String[] tokens = line.split("\\s+");
				ServerInfo temp = new ServerInfo(tokens[1], Integer.parseInt(tokens[2]));
				serverPool.add(temp);
			}
			fileReader.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	

	public static void main(String[] args) {
		int default_port = 4000;
		try {
			new LogSetup("logs/ecs/server.log", Level.ALL);
		} catch (IOException e1) {
			logger.error("Could not initialize logger");
		}
		
		if(args.length != 1) {
			logger.error("Eoor, invalid number of arguments!");
		} else {
			parseConfig(args[0]);
			new ECS(default_port).start();
		}
		
    }

}
