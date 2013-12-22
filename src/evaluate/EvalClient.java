package evaluate;

import java.io.IOException;
import java.net.ConnectException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;

import logger.LogSetup;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import client.KVStore;
import common.messages.KVMessage;
import common.messages.KVMessage.StatusType;

public class EvalClient implements Runnable {
	public static String defaultServer; //Alex Default Server
	public static int defaultPort;
	
	Eval evalInstance;
	private client.KVStore KVStore;
	private String name;
	private HashMap<String, String> kvMap = null;
	private ArrayList<String> keys = null;
	private ArrayList<String> getKeys = new ArrayList<String>();
	
	private int pSent = 0;
	private int pSuccess = 0;
	private int gSent = 0;
	private int gSuccess = 0;
	
	public Logger logger;
	public Logger mLog;
	
	public EvalClient(String name, Eval instance, String defaultServerIP, int defaultServerPort) {
		this.evalInstance = instance;
		this.name = name;
		defaultServer = defaultServerIP;
		defaultPort = defaultServerPort;
		
		LogSetup log = null;
		try {
			log = new LogSetup("logs/m.log", Level.INFO);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		this.mLog = log.getLogger();
	}
	
	public void run() {
		if (!Thread.interrupted()) {
			if (kvMap == null) {
				System.exit(1);
			}
			KVStore = new KVStore(defaultServer, defaultPort);
			try {
	            KVStore.connect();
				long tStart = 0;
				long tDuration = 0;
				long bitsSecond = 0;
				double clientTStart = (double) System.nanoTime() /  1000000;
				double clientTNext = clientTStart + 1000; 
				while (kvMap.size() > 0) {
					if (Thread.interrupted()) {
						KVStore.disconnect();
						return;
					}
					
					//Update Throughput
					if (((double)System.nanoTime() / 1000000) >= clientTNext) {
						evalInstance.getmInfo(this).updateThroughput(bitsSecond);
						bitsSecond = 0;
					}
					
					Random rand = new Random();
					int value = rand.nextInt(keys.size());
					tStart = System.nanoTime(); 
					KVMessage result = KVStore.put(keys.get(value), kvMap.get(keys.get(value)));
					pSent++;
					tDuration = System.nanoTime() - tStart; 
					
					//Redo if Error
					if (result.getStatus().equals(StatusType.PUT_ERROR)) {
						tStart = System.nanoTime(); 
						result = KVStore.put(keys.get(value), kvMap.get(keys.get(value)));
						pSent++;
						tDuration = System.nanoTime() - tStart; 
					}
					
					double elapsedTimePut = (double)tDuration/1000000;
				
					if (result != null && (result.getStatus().equals(StatusType.PUT_SUCCESS) || result.getStatus().equals(StatusType.PUT_UPDATE))) {
						pSuccess++;
						mLog.info("PUT Latency: " + elapsedTimePut);
						String dataSent = keys.get(value) + kvMap.get(keys.get(value));
						bitsSecond += dataSent.getBytes().length * 8; //Convert to Bits
						mLog.info("PUT bits added: " + bitsSecond + " for: " + dataSent);
						evalInstance.updateData(keys.get(value));
					} 
					
					kvMap.remove(keys.get(value));
					getKeys.add(keys.remove(value));
					
					int numAvailableKeys = evalInstance.getNumAvailableKeys();
					if (numAvailableKeys > 0) {
						value = rand.nextInt(evalInstance.getNumAvailableKeys());
						String randomKey = evalInstance.getAvailableKey(value);
						if (randomKey == null) {
							System.exit(1);
						}
						tStart = System.nanoTime();
						result = KVStore.get(randomKey);
						gSent++;
						tDuration = System.nanoTime() - tStart;
						
						//Redo if Error
						if (result.getStatus().equals(StatusType.PUT_ERROR)) {
							tStart = System.nanoTime();
							result = KVStore.get(randomKey);
							gSent++;
							tDuration = System.nanoTime() - tStart; 
						}
						
						double elapsedTimeGet = (double)tDuration/1000000; 
						if (result != null && (result.getStatus().equals(StatusType.GET_SUCCESS))) {
							gSuccess++;
							mLog.info("GET Latency: " + elapsedTimeGet);
							String dataRec = result.getValue() + result.getKey();
							bitsSecond += dataRec.getBytes().length * 8;
							mLog.info("GET bites added " + dataRec.getBytes().length * 8 + " for: " + dataRec);
						} 
						//Update Elapsed Time of PUT and GET
						evalInstance.getmInfo(this).update(elapsedTimePut, elapsedTimeGet,gSent,gSuccess, pSent,pSuccess);
					}
				}
				
			} catch (ConnectException e) {
				evalInstance.getLogger().error("ConnectException: " + e.getMessage());
			} catch (UnknownHostException e) {
				evalInstance.getLogger().error("UnknownHostException: " + e.getMessage());
				e.printStackTrace();
			} catch (IOException e) {
				evalInstance.getLogger().error("IOException: " + e.getMessage());
				e.printStackTrace();
			} 
		}
	}
	
	public KVStore getClient() {
		return this.KVStore;
	}
	
	public String getName() {
		return this.name;
	}
	
	public void setkvMap(HashMap<String, String> map) {
		this.kvMap = map;
		if (keys == null){
			keys = new ArrayList<String>();
		}	
		keys.clear();
		for (String key : kvMap.keySet()) {
			keys.add(key);
		}
	}
}
