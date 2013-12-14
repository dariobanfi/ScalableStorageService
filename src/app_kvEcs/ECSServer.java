package app_kvEcs;

import java.io.*;
import java.net.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import common.objects.ServerInfo;
import logger.LogSetup;


public class ECSServer implements ECS {
	
	static List <ServerInfo> iParameters = new ArrayList<ServerInfo>();
	
	private static Logger logger = Logger.getRootLogger();
    
    /**
     * Main entry point for the echo server application. 
     * @param args contains the port number at args[0].
     */

	@Override
	public void initService(int numberOfNodes) {
		String host = null;
		String port = null;
		// TODO Auto-generated method stub
		for(int x = 0; x < numberOfNodes; x++) {
			host = iParameters.get(x).getAddress();
			port = Integer.toString(iParameters.get(x).getPort());
			ProcessBuilder pb = new ProcessBuilder("nohup", "ssh", "-n", host, "java -jar", "ms3-server.jar", port, "&");
			pb.redirectErrorStream(); // redirect stderr to stdout
			try {
				Process process = pb.start();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	      }
	}

	@Override
	public void start() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void stop() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void shutDown() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void addNode() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void removeNode() {
		// TODO Auto-generated method stub
		
	}
	
	public static void main(String[] args) {
    	try {
			new LogSetup("logs/server.log", Level.ALL);
			if(args.length != 1) {
				System.out.println("Error! Invalid number of arguments!");
			} else {
				try {
					File file = new File(args[0]);
					FileReader fileReader = new FileReader(file);
					BufferedReader bufferedReader = new BufferedReader(fileReader);
					StringBuffer stringBuffer = new StringBuffer();
					String line;
					while ((line = bufferedReader.readLine()) != null) {
						String[] tokens = line.split("\\s+");
						ServerInfo temp = new ServerInfo();
						temp.setAddress(tokens[1]);
						temp.setPort(Integer.parseInt(tokens[2]));
						iParameters.add(temp);
					}
					fileReader.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
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
