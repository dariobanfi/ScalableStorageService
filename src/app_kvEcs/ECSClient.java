package app_kvEcs;

import java.io.*;
import java.net.*;
import logger.LogSetup;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;


/**
 *  Main client application
 *  It gives basic functionalities to connect to a Key-Value server, in order to
 *  add new values and query stored ones
 *
 **/

public class ECSClient{

        private static Logger logger = Logger.getRootLogger();
        private static final String PROMPT = "ECSClient> ";
        private BufferedReader stdin;
        private boolean stop = false;
        private ECSCommunicationLibrary ecs;
        private String serverAddress;
        private int serverPort;
        
        
        /**
         * This function is runs until the client decides to quit the program
         */
        public void run() {
                while(!stop) {
                        stdin = new BufferedReader(new InputStreamReader(System.in));
                        System.out.print(PROMPT);
                        
                        try {
                                String cmdLine = stdin.readLine();
                                this.handleCommand(cmdLine);
                        } catch (IOException e) {
                                stop = true;
                                printError("CLI does not respond - Application terminated ");
                        }
                }
        }
        
        private void handleCommand(String cmdLine) {
                String[] tokens = cmdLine.split("\\s+");

                if(tokens[0].equals("quit")) {        
                        stop = true;
                        if(ecs != null) {
                            try {
								ecs.disconnect();
							} catch (IOException e) {
							}
                            ecs = null;
                        }
                        System.out.println(PROMPT + "Application exit!");
                
                } else if (tokens[0].equals("connect")){
                        if(tokens.length == 3) {
                                try{
                                        serverAddress = tokens[1];
                                        serverPort = Integer.parseInt(tokens[2]);
                                        ecs = new ECSCommunicationLibrary(serverAddress,serverPort);
                                        System.out.println("Connected to ECSServer");
                                } catch(NumberFormatException nfe) {
                                        printError("No valid address. Port must be a number!");
                                        logger.info("Unable to parse argument <port>", nfe);
                                } catch (UnknownHostException e) {
                                        printError("Unknown Host!");
                                        logger.info("Unknown Host!", e);
                                } catch (IOException e) {
                                        printError("Could not establish connection!");
                                        logger.warn("Could not establish connection!", e);
                                }
                        } else {
                                printError("Invalid number of parameters!");
                        }
                        
                } 
                        
                else  if (tokens[0].equals("initservice")) {
                        if(tokens.length == 2) {
                                if(ecs != null){
                                        try {
											ecs.initService(Integer.parseInt(tokens[1]));
										} catch (NumberFormatException e) {
											System.out.println("Wrong number argument");
											e.printStackTrace();
										} catch (IOException e) {
											System.out.println("Problem connecting");
										}
                                } else {
                                        printError("Not connected!");
                                }
                        } else {
                                printError("Usage: initservice <number>");
                        }
                        
                }
                
                else if(tokens[0].equals("start")) {
                    if(ecs != null) {
                        try {
							ecs.start();
						} catch (IOException e) {
							System.out.println("Error sending command");
						}
                    }
                    else{
                    	System.out.println("You are not connected to the ECS Server");
                    }
                }
                
                else if(tokens[0].equals("stop")) {
                    if(ecs != null) {
                        try {
							ecs.stop();
						} catch (IOException e) {
							System.out.println("Error sending command");
						}
                    }
                    else{
                    	System.out.println("You are not connected to the ECS Server");
                    }
                } 
                
                else if(tokens[0].equals("addnode")) {
                    if(ecs != null) {
                        try {
							ecs.addNode();
						} catch (IOException e) {
							System.out.println("Error sending command");
						}
                    }
                    else{
                    	System.out.println("You are not connected to the ECS Server");
                    }
                } 
                
                else if(tokens[0].equals("removenode")) {
                    if(ecs != null) {
                        try {
							ecs.removeNode();
						} catch (IOException e) {
							System.out.println("Error sending command");
						}
                    }
                    else{
                    	System.out.println("You are not connected to the ECS Server");
                    }
                } 
               
                else if(tokens[0].equals("shutdown")) {
                    if(ecs != null) {
                        try {
							ecs.shutDown();
						} catch (IOException e) {
							System.out.println("Error sending command");
						}
                    }
                    else{
                    	System.out.println("You are not connected to the ECS Server");
                    }
                } 
                
                else if(tokens[0].equals("disconnect")) {
                    if(ecs != null) {
                        try {
							ecs.disconnect();
						} catch (IOException e) {
							System.out.println("Error sending command");
						}
                        ecs = null;
                    }
                    else{
                    	System.out.println("You are not connected to the ECS Server");
                    }
                } 
                        
                else if(tokens[0].equals("help")) {
                        printHelp();
                } 
                
                else {
                        printError("Unknown command");
                        printHelp();
                }
        }
        

        

        
        private void printHelp() {
                StringBuilder sb = new StringBuilder();
                sb.append(PROMPT).append("ECHO CLIENT HELP (Usage):\n");
                sb.append(PROMPT);
                sb.append("::::::::::::::::::::::::::::::::");
                sb.append("::::::::::::::::::::::::::::::::\n");
                sb.append(PROMPT).append("connect <host> <port>");
                sb.append("\t establishes a connection to a server\n");
                sb.append(PROMPT).append("initservice <number>");
                sb.append("\t\t Initializes n random KVServers\n");
                
                sb.append(PROMPT).append("start"); 
                sb.append("\t\t All KVServer start to accept client requests\n");
                
                sb.append(PROMPT).append("stop");
                sb.append("\t\t All KVServer stop to accept client requests\n");
                
                sb.append(PROMPT).append("shutdown");
                sb.append("\t\t Shuts down all the KVServers\n");
                
                sb.append(PROMPT).append("addnode");
                sb.append("\t\tAdd a new node to the storage service at an arbitrary position.\n");
                
                sb.append(PROMPT).append("removenode");
                sb.append("\t\t Remove a node from the storage service at an arbitrary position\n");
                
                sb.append(PROMPT).append("disconnect");
                sb.append("\t\t\t disconnects from the server \n");
                
                sb.append(PROMPT).append("quit ");
                sb.append("\t\t\t exits the program");
                System.out.println(sb.toString());
        }
    
        private void printError(String error){
                System.out.println(PROMPT + "Error! " +  error);
        }

    public static void main(String[] args) {
            try {
	                new LogSetup("logs/ecs/ecsclient.log", Level.OFF);
	                ECSClient app = new ECSClient();
	                app.run();
                } catch (IOException e) {
                    System.out.println("Error! Unable to initialize logger!");
                    e.printStackTrace();
                    System.exit(1);
                }
    }


}