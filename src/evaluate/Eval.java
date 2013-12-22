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
	
	private int numClients;
	private int numRequestsPerClient;
	private String enronPath = "";
	private HashMap<EvalClient, HashMap<String, String>> requestMap; 
	private ConcurrentHashMap<Integer, String> availableData; 
	private HashMap<Integer, String> indexMap; 
	private HashMap<EvalClient, Measurement> mMap; 
	private HashMap<String, String> kvMap;	
	private HashMap<Integer, String> enronFiles; 
	private ArrayList<EvalClient> clients;	
	private ArrayList<Thread> clientThreads;
	public Logger mLogger;
	private ServerInfo temp_info;
	
	public static final String ENRON_KEY = "Message-ID: "; // KEY Identifier of Enron Emails
	public static final String ENRON_VALUE = "Subject: "; // Value Identifier of Enron Emails

	public Eval(String enronPath, int numClients, int numServers, int numRequestsPerClient) {
		this.enronPath = enronPath;
		this.numClients = numClients;
		this.numRequestsPerClient = numRequestsPerClient;
			
		clients = new ArrayList<EvalClient>();
		kvMap = new HashMap<String, String>();
		indexMap = new HashMap<Integer, String>();
		requestMap = new HashMap<EvalClient, HashMap<String, String>>();
		enronFiles = new HashMap<Integer, String>();
		mMap = new HashMap<EvalClient, Measurement>();
		availableData = new ConcurrentHashMap<Integer, String>();
		
		LogSetup ls2 = null;
		try {
			ls2 = new LogSetup("logs/mData-" + numClients + "-" + numServers + "-" + numRequestsPerClient + ".log", Level.ALL);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		this.mLogger = ls2.getLogger();
		
		startServers(numServers);
		
		for (int i = 0; i < this.numClients; i++) {
			clients.add(new EvalClient("CLIENT " + i, this));
		}
		
		for (EvalClient client : clients) {
			mMap.put(client, new Measurement());
		}
		
		initEnron(enronPath);
		
	}
	
	public synchronized String getAvailableKey(int id) {
		
		if (availableData.containsKey(id)) {
			return availableData.get(id);
		} else {			
			for (Entry<Integer, String> entry : availableData.entrySet()) {
				System.out.println("ID: " + entry.getKey() + ", Key: " + entry.getValue());
			}
			
			return null;
		}
	}
	
	public synchronized void updateData(String key) {
		if (!availableData.containsKey(key)) {
			availableData.put(availableData.size(), key);
		}
	}
	
	public Logger getLogger() {
		return this.mLogger;
	}
	
	
	public synchronized int getNumAvailableKeys() {
		return availableData.size();
	}

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
				File enronFile = new File(enronFiles.get(i));
				contents = new BufferedReader (new FileReader(enronFile));
				
				try { 
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
							break; 
					}
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
			}
		}
		System.out.println("Total number of files loaded:"+kvMap.size());
		populate();
	}
	
	public void processFiles(File[] files, int i) {
        for (File file : files) {
            if (file.isDirectory()) {
                processFiles(file.listFiles(), i); 
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
	
	public Measurement getmInfo(EvalClient client) {
		if (mMap.containsKey(client)) {
			return mMap.get(client);
		} else {
			return null;
		}
	}
	
	private void populate() {
		if (kvMap == null || kvMap.size() <= 0) {
			return;
		}
		
		for (EvalClient client : clients) {
			if (!requestMap.containsKey(client)) {
				requestMap.put(client, new HashMap<String, String>());
			}
			
			int numRequestPairs = kvMap.size();
			Random rand = new Random();
			
			for (int i = 0; i < numRequestsPerClient; i++) {
				int rval = rand.nextInt(numRequestPairs);
				String key = indexMap.get(rval);
				if (key != null && !key.equals("")) {
					String value = kvMap.get(key);
					HashMap<String, String> clientRequests = requestMap.get(client);
					if (clientRequests != null) {
						clientRequests.put(key, value);
					} 
				} 
			} 
		} 
		
	}
	
	public void start(String address, int port) throws ConnectException, UnknownHostException, IOException {	
		clientThreads = new ArrayList<Thread>();
		
		for (EvalClient client : clients) {
			client.setRequestMap(requestMap.get(client));
			Thread t = new Thread(client);
			t.setName(client.getName() + " " + t.getName());
			clientThreads.add(t);
			t.start();
		}
	}
	
	public void processMData() {
		double avgLatencyGet = 0;
		double avgLatencyPut = 0;
		double avgThroughput = 0;
		
		for (EvalClient client : clients) {
			Measurement mInfo = mMap.get(client);
			
			avgLatencyGet += mInfo.getLatencyGet();
			avgLatencyPut += mInfo.getLatencyPut();
			avgThroughput += mInfo.getThroughput();
		}
		
		avgLatencyGet /= clients.size();
		avgLatencyPut /= clients.size();
		
	mLogger.info(avgLatencyGet + "\t" + avgLatencyPut + "\t" + avgThroughput);
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
		Eval evaluator = new Eval(args[0], 5, 3, 200);
		
		try {
			evaluator.start("10.211.55.18", 5000); //Default for Alex, "127.0.0.1" for all others
			
			// wait for threads to conclude
			for (Thread ethread : evaluator.clientThreads) {
				try {
					ethread.join();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			
			evaluator.processMData();
		} catch (ConnectException e) {
			e.printStackTrace();
		} catch (UnknownHostException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} 
		
		System.exit(0);
	}
}
