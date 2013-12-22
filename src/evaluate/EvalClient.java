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

import client.KVCommInterface;
import client.KVStore;
import common.messages.KVMessage;
import common.messages.KVMessage.StatusType;
import app_kvClient.KVClient;

public class EvalClient implements Runnable {
	public static String defaultServer; //Alex Default Server
	public static int defaultPort;
	
	Eval evalInstance;
	private client.KVStore KVStore;
	private String name;
	private HashMap<String, String> requestMap = null;
	private ArrayList<String> keys = null;
	private ArrayList<String> getKeys = new ArrayList<String>();
	
	private int pSent = 0;
	private int pSuccess = 0;
	private int pFailed = 0;
	private int gSent = 0;
	private int gSuccess = 0;
	private int gFailed = 0;
	
	public Logger logger;
	public Logger mLog;
	
	public EvalClient(String name, Eval instance, String defaultServerIP, int defaultServerPort) {
		this.evalInstance = instance;
		this.name = name;
		defaultServer = defaultServerIP;
		defaultPort = defaultServerPort;
		
		LogSetup log = null;
		try {
			log = new LogSetup("logs/perf.log", Level.INFO);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		this.mLog = log.getLogger();
	}
	
	public void run() {
		if (!Thread.interrupted()) {
			if (requestMap == null) {
				System.exit(1);
			}
			
			KVStore = new KVStore(defaultServer, defaultPort);
	
			try {
				
	            KVStore.connect();
				
				long startTime = 0;
				long elapsedTime = 0;
				long bitsSecond = 0;
				double timerStart = (double) System.nanoTime() /  1000000;
				double timerNextPing = timerStart + 1000; 
				while (keys.size() > 0) {
					if (Thread.interrupted()) {
						KVStore.disconnect();
						return;
					}

					if (((double)System.nanoTime() / 1000000) >= timerNextPing) {
						evalInstance.getmInfo(this).updateThroughput(bitsSecond);
						bitsSecond = 0;
					}
					
					Random rand = new Random();
					int value = rand.nextInt(keys.size());
					startTime = System.nanoTime(); 
					KVMessage result = KVStore.put(keys.get(value), requestMap.get(keys.get(value)));
					elapsedTime = System.nanoTime() - startTime; 
					double elapsedTimePut = (double)elapsedTime / 1000000;
					
					requestMap.remove(keys.get(value));
					getKeys.add(keys.remove(value));
					
					pSent++;
					if (result != null && (result.getStatus().equals(StatusType.PUT_SUCCESS) || result.getStatus().equals(StatusType.PUT_UPDATE))) {
						mLog.info("PUT Latency: " + elapsedTimePut);
						pSuccess++;
						String dataSent = keys.get(value) + requestMap.get(keys.get(value));
						bitsSecond += dataSent.getBytes().length * 8;
						mLog.info("PUT Bytes added: " + dataSent.getBytes().length * 8 + "for: " + dataSent);
						evalInstance.updateData(keys.get(value));
					} else {
						pFailed++;
						mLog.info(result.getStatus()+" - PUT Latency: " + elapsedTimePut);
					}
					
					int numAvailableKeys = evalInstance.getNumAvailableKeys();
					if (numAvailableKeys > 0) {
						value = rand.nextInt(evalInstance.getNumAvailableKeys());
						String randomKey = evalInstance.getAvailableKey(value);
						
						if (randomKey == null) {
							System.exit(1);
						}
						
						startTime = System.nanoTime();
						result = KVStore.get(randomKey);				
						elapsedTime = System.nanoTime() - startTime; 
						
						if (result.getStatus().equals(StatusType.GET_ERROR)) {
							startTime = System.nanoTime();
							result = KVStore.get(randomKey);
							elapsedTime = System.nanoTime() - startTime; 
						}
						
						double elapsedTimeGet = (double)elapsedTime / 1000000; 
						
						gSent++;
						if (result != null && (result.getStatus().equals(StatusType.GET_SUCCESS))) {
							mLog.info("GET Latency: " + elapsedTimeGet);
							gSuccess++;
							String dataRec = result.getValue() + result.getKey();
							bitsSecond += dataRec.getBytes().length * 8;
							mLog.info("GET Bytes: " + dataRec.getBytes().length * 8 + " for: " + dataRec);
						} else {
							gFailed++;
							mLog.info(result.getStatus()+" - GET latency: " + elapsedTimeGet);
						}
						
						evalInstance.getmInfo(this).update(elapsedTimePut, elapsedTimeGet);
					}
				}
				
				double LatencyGetAvg = Math.round(evalInstance.getmInfo(this).getLatencyGet());
				double LatencyPutAvg = Math.round(evalInstance.getmInfo(this).getLatencyPut());
				double bpsAvg = Math.round(evalInstance.getmInfo(this).getThroughput());
				
				mLog.info(this.name + " finished. " + pSuccess + "/" + pSent + "; " + gSuccess + "/" + gSent);
				mLog.info("Average Get Latency: " + LatencyGetAvg + ", Average Put Latency: " + LatencyPutAvg + ", Average Throughput: " + bpsAvg);
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
	
	public void setRequestMap(HashMap<String, String> map) {
		this.requestMap = map;
		
		if (keys == null)
			keys = new ArrayList<String>();
		
		keys.clear();
		for (String key : requestMap.keySet()) {
			keys.add(key);
		}
	}
}
