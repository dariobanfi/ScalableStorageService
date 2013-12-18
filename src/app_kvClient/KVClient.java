package app_kvClient;

import java.io.*;
import java.net.*;

import logger.LogSetup;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import common.messages.KVMessage;

import client.KVCommInterface;
import client.KVStore;


/**
 *  Main client application
 *  It gives basic functionalities to connect to a Key-Value server, in order to
 *  add new values and query stored ones
 *
 **/

public class KVClient{

        private static Logger logger = Logger.getRootLogger();
        private static final String PROMPT = "KVServerClient> ";
        private BufferedReader stdin;
        private boolean stop = false;
        private KVCommInterface KVStore;
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
                        disconnect();
                        System.out.println(PROMPT + "Application exit!");
                
                } else if (tokens[0].equals("connect")){
                        if(tokens.length == 3) {
                                try{
                                        serverAddress = tokens[1];
                                        serverPort = Integer.parseInt(tokens[2]);
                                        KVStore = new KVStore(serverAddress,serverPort);
                                        connect(serverAddress, serverPort);
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
                
                else  if (tokens[0].equals("put")) {
                        if(tokens.length >= 3) {
                                if(KVStore != null){
                                        StringBuilder msg = new StringBuilder();
                                        for(int i = 2; i < tokens.length; i++) {
                                                msg.append(tokens[i]);
                                                if (i != tokens.length -1 ) {
                                                        msg.append(" ");
                                                }
                                        }        
                                        put(tokens[1], msg.toString());
                                } else {
                                        printError("Not connected!");
                                }
                        } else {
                                printError("Usage: put <key> <value>");
                        }
                }
                        
                else  if (tokens[0].equals("get")) {
                        if(tokens.length == 2) {
                                if(KVStore != null){
                                        get(tokens[1]);
                                } else {
                                        printError("Not connected!");
                                }
                        } else {
                                printError("Usage: get <key>");
                        }
                        
                } else if(tokens[0].equals("disconnect")) {
                        disconnect();
                        
                } else if(tokens[0].equals("logLevel")) {
                        if(tokens.length == 2) {
                                String level = setLevel(tokens[1]);
                                if(level.equals(LogSetup.UNKNOWN_LEVEL)) {
                                        printError("No valid log level!");
                                        printPossibleLogLevels();
                                } else {
                                        System.out.println(PROMPT + 
                                                        "Log level changed to level " + level);
                                }
                        } else {
                                printError("Invalid number of parameters!");
                        }
                        
                } else if(tokens[0].equals("help")) {
                        printHelp();
                } else {
                        printError("Unknown command");
                        printHelp();
                }
        }
        
        private void put(String key, String value){        
                try {
                        KVMessage response = KVStore.put(key, value);
                        if(response.getValue()!=null){
                                System.out.println(response.getStatus() + " " + response.getValue());
                        }
                        else{
                                System.out.println(response.getStatus());
                        }
                } catch (IOException e) {
                        System.out.println("Unable to put the value in the server");
                        logger.error(e.toString());
                }
        }
        
        private void get(String key){        
                try {
                        KVMessage response = KVStore.get(key);
                        if(response.getStatus().equals(KVMessage.StatusType.GET_SUCCESS)){
                                System.out.println(response.getStatus() + ": " + response.getValue() );
                        }
                        else{
                                System.out.println(response.getStatus());
                        }
                } catch (Exception e) {
                        System.out.println(e.toString());
                        logger.error(e.toString());
                }
        }

        private void connect(String address, int port) 
                        throws UnknownHostException, IOException {
                
                KVStore = new KVStore(serverAddress,serverPort);
                try {
                        KVStore.connect();
                        System.out.println("Connected to " + serverAddress + "/"+ port);
                } catch (Exception e) {
                        System.out.println("Error while trying to connect");
                        logger.error("Unable to connect");
                }
        }
        
        private void disconnect() {
                if(KVStore != null) {
                        KVStore.disconnect();;
                        KVStore = null;
                }
        }
        
        private void printHelp() {
                StringBuilder sb = new StringBuilder();
                sb.append(PROMPT).append("KV CLIENT HELP (Usage):\n");
                sb.append(PROMPT);
                sb.append("::::::::::::::::::::::::::::::::");
                sb.append("::::::::::::::::::::::::::::::::\n");
                sb.append(PROMPT).append("connect <host> <port>");
                sb.append("\t establishes a connection to a server\n");
                sb.append(PROMPT).append("put <key> <value>");
                sb.append("\t\t Saves the value <value> in the position <key>. If already present, it updates"
                                + "the key value, if set to empty, it deletes it.\n");
                sb.append(PROMPT).append("get <key>");
                sb.append("\t\t Returns the value saved in <key> position \n");
                sb.append(PROMPT).append("disconnect");
                sb.append("\t\t\t disconnects from the server \n");
                
                sb.append(PROMPT).append("logLevel");
                sb.append("\t\t\t changes the logLevel \n");
                sb.append(PROMPT).append("\t\t\t\t ");
                sb.append("ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF \n");
                
                sb.append(PROMPT).append("quit ");
                sb.append("\t\t\t exits the program");
                System.out.println(sb.toString());
        }
        
        private void printPossibleLogLevels() {
                System.out.println(PROMPT 
                                + "Possible log levels are:");
                System.out.println(PROMPT 
                                + "ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF");
        }

        private String setLevel(String levelString) {
                
                if(levelString.equals(Level.ALL.toString())) {
                        logger.setLevel(Level.ALL);
                        return Level.ALL.toString();
                } else if(levelString.equals(Level.DEBUG.toString())) {
                        logger.setLevel(Level.DEBUG);
                        return Level.DEBUG.toString();
                } else if(levelString.equals(Level.INFO.toString())) {
                        logger.setLevel(Level.INFO);
                        return Level.INFO.toString();
                } else if(levelString.equals(Level.WARN.toString())) {
                        logger.setLevel(Level.WARN);
                        return Level.WARN.toString();
                } else if(levelString.equals(Level.ERROR.toString())) {
                        logger.setLevel(Level.ERROR);
                        return Level.ERROR.toString();
                } else if(levelString.equals(Level.FATAL.toString())) {
                        logger.setLevel(Level.FATAL);
                        return Level.FATAL.toString();
                } else if(levelString.equals(Level.OFF.toString())) {
                        logger.setLevel(Level.OFF);
                        return Level.OFF.toString();
                } else {
                        return LogSetup.UNKNOWN_LEVEL;
                }
        }


        private void printError(String error){
                System.out.println(PROMPT + "Error! " +  error);
        }
        
    /**
     * Main entry point for the echo server application. 
     * @param args contains the port number at args[0].
     */
    public static void main(String[] args) {
            try {
                        new LogSetup("logs/client/client.log", Level.OFF);
                        KVClient app = new KVClient();
                        app.run();
                } catch (IOException e) {
                        System.out.println("Error! Unable to initialize logger!");
                        e.printStackTrace();
                        System.exit(1);
                }
    }


}