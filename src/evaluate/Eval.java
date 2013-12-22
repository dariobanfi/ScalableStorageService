package evaluate;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import logger.LogSetup;

import org.apache.commons.io.*;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import common.messages.*;
import common.objects.Metadata;
import common.objects.ServerInfo;
import app_kvClient.KVClient;
import app_kvEcs.ECSClient;
import app_kvEcs.ECSServer;
import app_kvServer.KVServer;

public class Eval {
	
	public static final String ENRON_KEY = "Message-ID: "; // KEY Identifier of Enron Emails
	public static final String ENRON_VALUE = "Subject: "; // Value Identifier of Enron Emails
	
	private int numClients;
	private int numRequestsPerClient;
	private String enronPath = "";
	
	private ArrayList<EvalClient> clients;	
	private ArrayList<Thread> clientThreads;
	private HashMap<EvalClient, HashMap<String, String>> requestMap;
	private HashMap<Integer, String> indexMap;
	private HashMap<String, String> kvMap;	
	private HashMap<Integer, String> enronFiles; 
	private HashMap<EvalClient, Measurement> perfMap; 
	private ConcurrentHashMap<Integer, String> availableData; 
	public Logger perfLogger;
	private ServerInfo temp_info;

	
	public Eval(String enronPath, int numClients, int numServers, int numRequestsPerClient) {
		this.enronPath = enronPath;
		this.numClients = numClients;
		this.numRequestsPerClient = numRequestsPerClient;
			
		clients = new ArrayList<EvalClient>();
		kvMap = new HashMap<String, String>();
		indexMap = new HashMap<Integer, String>();
		requestMap = new HashMap<EvalClient, HashMap<String, String>>();
		enronFiles = new HashMap<Integer, String>();
		perfMap = new HashMap<EvalClient, Measurement>();
		availableData = new ConcurrentHashMap<Integer, String>();
		
		LogSetup ls2 = null;
		try {
			ls2 = new LogSetup("logs/perfData-" + numClients + "-" + numServers + "-" + numRequestsPerClient + ".log", Level.ALL);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		this.perfLogger = ls2.getLogger();
		
		for (int i = 0; i < this.numClients; i++) {
			clients.add(new EvalClient("CLIENT " + i, this));
		}
		
		for (EvalClient client : clients) {
			perfMap.put(client, new Measurement());
		}
		
		initEnron(enronPath);
		startServers(numServers);
	}
	
	public synchronized void updateData(String key) {
		if (!availableData.containsKey(key)) {
			availableData.put(availableData.size(), key);
		}
	}
	
	public Logger getLogger() {
		return this.perfLogger;
	}
	
	
	public synchronized int getNumAvailableKeys() {
		return availableData.size();
	}
	
	public synchronized String getAvailableKey(int id) {
	
		if (availableData.containsKey(id)) {
			return availableData.get(id);
		} else {
			System.out.println("available Data does not contain " + id);
			
			for (Entry<Integer, String> entry : availableData.entrySet()) {
				System.out.println("ID: " + entry.getKey() + ", Key: " + entry.getValue());
			}
			
			return null;
		}
	}
	
	/**
	 * Get the perforamance Info for a wrapper
	 * @param client the wrapper
	 * @return the performance info for this wrapper
	 */
	public Measurement getPerfInfo(EvalClient client) {
		if (perfMap.containsKey(client)) {
			return perfMap.get(client);
		} else {
			return null;
		}
	}
	

	
	/**
	 * Initializes Enron files for processing and obtains the specified number of key-value data of specified format from the Enron dataset.
	 */
	public void initEnron(String enronPath) {
		
		try {
			File[] files = new File(enronPath).listFiles();
			System.out.println("Importing files ..... please be patient");
    		processFiles(files,0);
		} catch (IllegalArgumentException ex) {
			System.exit(1);
		}
	
		BufferedReader contents;
	
		for (int i = 0; i < enronFiles.size(); i++) {
			try {
				
				// Obtain file contents for current file in set
				File enronFile = new File(enronFiles.get(i));
				contents = new BufferedReader (new FileReader(enronFile));
				
				try { // read the file and try to obtain the specified key->value data.
					String line = null;
					String valKey = "";
					String valValue = "";
					
					while ((line = contents.readLine()) != null) {
						if (line.startsWith(ENRON_KEY) && valKey.equals("")) {
							valKey = line.substring(ENRON_KEY.length());
							valKey.replaceAll("\\s+","");
						} else if (line.startsWith(ENRON_VALUE) && valValue.equals("")) {
							valValue = line.substring(ENRON_VALUE.length());
						}
						
						if (!valKey.equals("") && !valValue.equals(""))
							break; // If we found both key and value, stop reading lines from the file.
					}
					
					// If we obtained Key-Value data from the file, add it to the KVMap
					if (!valKey.equals("") && !valValue.equals("")) {
						if (!kvMap.containsKey(valKey)) {
							indexMap.put(kvMap.size(), valKey);
							kvMap.put(valKey, valValue);
						}
					}
				} finally {
					contents.close();
				}
			} catch (IOException ex) {
				//logger.error("IOException occured while trying to read from enron file: " + enronFiles.get(i));
			}
		}
		System.out.println("Total number of files loaded:"+kvMap.size());
		populateRequestTables();
	}
	
	public void processFiles(File[] files, int i) {
        for (File file : files) {
            if (file.isDirectory()) {
                //System.out.println("Directory: " + file.getName());
                processFiles(file.listFiles(), i); // Calls same method again.
            } else {
                try {
                	  int len;
          		      char[] chr = new char[8192];
          		      final StringBuffer buffer = new StringBuffer();
          		      final FileReader reader = new FileReader(file);
          		      try {
          		          while ((len = reader.read(chr)) > 0) {
          		              buffer.append(chr, 0, len);
          		          }
          		      } finally {
          		          reader.close();
          		      }
          		    enronFiles.put(i, file.getPath());
          		    //System.out.println(file.getPath());
          		    i++;
				} catch (IOException e) {
					e.printStackTrace();
				}
            }
        }
    }
	
	private void populateRequestTables() {
		System.out.println("Populating Request Tables for all " + clients.size() + " Clients.");
		if (kvMap == null || kvMap.size() <= 0) {
			System.out.println("Unable to populate request tables, since request map does not contain any entries.");
			return;
		}
		
		for (EvalClient client : clients) {
			// If client not in request table, then insert it
			if (!requestMap.containsKey(client)) {
				requestMap.put(client, new HashMap<String, String>());
			}
			
			// get number of entries from request map
			int numRequestPairs = kvMap.size();
			Random rand = new Random();
			
			
			for (int i = 0; i < numRequestsPerClient; i++) {
				// get a random request pair from request map
				int randomPair = rand.nextInt(numRequestPairs);
				
				// Obtain key for this index
				String key = indexMap.get(randomPair);
				
				// make sure key is valid
				if (key != null && !key.equals("")) {
					// Obtain value for key
					String value = kvMap.get(key);
					
					// add the key value pair to the clients request map
					HashMap<String, String> clientRequests = requestMap.get(client);
					if (clientRequests != null) {
						clientRequests.put(key, value);
					} else {
						System.out.println("Unable to add Request with key: \"" + key + "\" and value \"" + value + "\" to\n" + "request table for client. The Clients request table was null.");
					}
				} else {
					System.out.println("Unable to add random pair with ID: " + randomPair + ". Unable to obtain key\n" + "from index map. Size of index map: " + indexMap.size() + ", size of kvMap: " + kvMap.size()); 
				}
			} // end for number of requests
		} // end foreach client
		
	}
	
	public void start(String address, int port) throws ConnectException, UnknownHostException, IOException {	
		clientThreads = new ArrayList<Thread>();
		
		for (EvalClient client : clients) {
			client.setRequestMap(requestMap.get(client));
			Thread t = new Thread(client);
			t.setName(client.getName() + " " + t.getName());
			clientThreads.add(t);
			t.start();
			//logger.warn("Started client thread " + t);
		}
	}
	
	public void processPerfData() {
		double avgLatencyGet = 0.0;
		double avgLatencyPut = 0.0;
		double avgThroughput = 0.0;
		
		for (EvalClient client : clients) {
			Measurement perfInfo = perfMap.get(client);
			
			avgLatencyGet += perfInfo.getLatencyGet();
			avgLatencyPut += perfInfo.getLatencyPut();
			avgThroughput += perfInfo.getThroughpout();
		}
		
		avgLatencyGet /= clients.size();
		avgLatencyPut /= clients.size();
		
		//System.out.println("Avg Latency Get: " + avgLatencyGet + ", Avg Latency Put: " + avgLatencyPut + ", Avg Throughput bps: " + avgThroughput);
		perfLogger.info(avgLatencyGet + "\t" + avgLatencyPut + "\t" + avgThroughput);
	}
	
	public ECSServer startServers(int numberOfServers) {
		ECSServer ecs = new ECSServer("settings.alex.config");		
		ecs.initService(numberOfServers);
		ecs.start();
		return ecs;
	}
	
	public void stopServers(ECSServer ecs) {
		ecs.stop();
	}
	
	public static void main(String[] args) {

		if (args.length != 1) {
			System.out.println("Invalid Argument");
			System.exit(1);
		}
		
		/*		 
		 * Evaluator, 
		 * Arg 1 - maildir path of Enron Data
		 * Arg 2 - number of clients
		 * Arg 3 - number of servers
		 * Arg 5 - number of requests for each client 
		 */
		Eval evaluator = new Eval(args[0], 20, 1, 200);
		
		try {
			evaluator.start("10.211.55.18", 5000); //Default for Alex, "127.0.0.1" for all others
			
			// wait for all threads to conclude
			for (Thread ethread : evaluator.clientThreads) {
				try {
					ethread.join();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			
			// output performance data to file
			evaluator.processPerfData();
		} catch (ConnectException e) {
			//System.out.println(e.getMessage());
			e.printStackTrace();
		} catch (UnknownHostException e) {
			//System.out.println(e.getMessage());
			e.printStackTrace();
		} catch (IOException e) {
			//System.out.println(e.getMessage());
			e.printStackTrace();
		} 
		
		System.exit(0);
	}
}
