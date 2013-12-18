package app_kvEcs;

import java.io.*;


/**
 *  Main client application used to control the 
 *  underlying ECSServer implementation by sending
 *  him commands
 **/

public class ECSClient{

        private static final String PROMPT = "ECSClient> ";
        private BufferedReader stdin;
        private boolean stop = false;
        private ECSClientInterface ecs;
        
        public ECSClient(){
        	ecs = new ECSServer();
        }
        /**
         * This function is runs until the client decides to quit the program
         */
        public void run() {
        		System.out.println("Welcome to the ECS CLI Inteface");
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
                        System.out.println(PROMPT + "Application exit!");
                } 
                else  if (tokens[0].equals("initservice")) {
                        if(tokens.length == 2) {
                        	int val = Integer.parseInt(tokens[1]);
                        	try {
								if(ecs.initService(val))
									System.out.println("Sent INITSERVICE command");
								else
									System.out.println("Service already initiated, use \"addnode\" or shutdown"
											+ "all servers and call initservice again");
							} catch (IOException e) {
								System.out.println("Error sending command");
							}
                        } else {
                                printError("Usage: initservice <number>");
                        }
                        
                }
                else if(tokens[0].equals("start")) {
                    try {
						ecs.start();
						System.out.println("Sent START command");
					} catch (IOException e) {
						System.out.println("Error sending command");
					}
                } 
                
                else if(tokens[0].equals("stop")) {
                    try {
						ecs.stop();
						System.out.println("Sent STOP command");
					} catch (IOException e) {
						System.out.println("Error sending command");
					}
                } 
                
                else if(tokens[0].equals("addnode")) {
                    try {
						ecs.addNode();
						System.out.println("Sent ADD_NODE command");
					} catch (IOException e) {
						System.out.println("Error sending command");
					}
                } 
                
                else if(tokens[0].equals("removenode")) {
                    try {
						ecs.removeNode();
						System.out.println("Sent REMOVE_NODE command");
					} catch (IOException e) {
						System.out.println("Error sending command");
					}
                } 
               
                else if(tokens[0].equals("shutdown")) {
                    try {
						ecs.shutDown();
						System.out.println("Sent SHUTDOWN command");
					} catch (IOException e) {
						System.out.println("Error sending command");
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
                
                sb.append(PROMPT).append("initservice <numberOfNodes>");
                sb.append("\t\tRandomly choose <numberOfNodes> servers from the available machines "
                		+ "and start the KVServer by issuing a SSH call to the respective machine."
                		+ "This call launches the server.\n");
                
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
                
                sb.append(PROMPT).append("quit ");
                sb.append("\t\t\t Exits the program");
                System.out.println(sb.toString());
        }
    
        private void printError(String error){
                System.out.println(PROMPT + "Error! " +  error);
        }


}