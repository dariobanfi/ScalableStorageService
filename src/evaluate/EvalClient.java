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
	public static String defaultServer = "10.211.55.18"; //Alex Default Server
	public static int defaultPort = 50000;
	
	Eval evalInstance;
	private client.KVStore KVStore;
	private String name;
	private HashMap<String, String> requestMap = null;
	private ArrayList<String> keys = null;
	private ArrayList<String> getKeys = new ArrayList<String>();
	private int putsSent = 0;
	private int putsSuccess = 0;
	private int putsFailed = 0;
	
	private int getsSent = 0;
	private int getsSuccess = 0;
	private int getsFailed = 0;
	
	public Logger logger;
	public Logger perfLog;
	
	public EvalClient(String name, Eval instance) {
		this.evalInstance = instance;
		this.name = name;
		
		LogSetup log = null;
		try {
			log = new LogSetup("logs/perf.log", Level.INFO);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		this.perfLog = log.getLogger();
	}
	
	public void run() {
		if (!Thread.interrupted()) {
			if (requestMap == null) {
				evalInstance.getLogger().warn(this.getName() + " has an empty request map. Exiting.");
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
					
					// report bps each second
					if (((double)System.nanoTime() / 1000000) >= timerNextPing) {
						evalInstance.getPerfInfo(this).updateThroughput(bitsSecond);
						bitsSecond = 0;
					}
					
					/* Put a random key value pair from the clients kv map */
					Random rand = new Random();
					int value = rand.nextInt(keys.size());
					
					startTime = System.nanoTime(); 
					
					KVMessage result = KVStore.put(keys.get(value), requestMap.get(keys.get(value)));
					elapsedTime = System.nanoTime() - startTime; // elapsed time in nano seconds
					double elapsedTimePut = (double)elapsedTime / 1000000; // to ms
					
					requestMap.remove(keys.get(value));
					getKeys.add(keys.remove(value));
					
					putsSent++;
					if (result != null && (result.getStatus().equals(StatusType.PUT_SUCCESS) || result.getStatus().equals(StatusType.PUT_UPDATE))) {
						System.out.println("Successfully put <" + keys.get(value) + ", " + requestMap.get(keys.get(value)) + "> (" + result.getStatus().toString() + ")");
						perfLog.info("Put latency: " + elapsedTimePut);
						putsSuccess++;
						
						String dataSent = keys.get(value) + requestMap.get(keys.get(value));
						bitsSecond += dataSent.getBytes().length * 8;
						
						perfLog.info("PUT BPS added: " + dataSent.getBytes().length * 8 + "for: " + dataSent);
						
						evalInstance.updateData(keys.get(value));
					} else {
						putsFailed++;
						perfLog.info(result.getStatus()+" - Put latency: " + elapsedTimePut);
					}
					
					int numAvailableKeys = evalInstance.getNumAvailableKeys();
					if (numAvailableKeys > 0) {
						value = rand.nextInt(evalInstance.getNumAvailableKeys());
						String randomKey = evalInstance.getAvailableKey(value);
						
						if (randomKey == null) {
							evalInstance.getLogger().error(name + " TRIED TO GET INVALID KEY: " + value);
							System.exit(1);
						}
						
						startTime = System.nanoTime();
						// evalInstance.getLogger().info("Trying to get " + randomKey);
						result = KVStore.get(randomKey);				
						elapsedTime = System.nanoTime() - startTime; // elapsed time in nano seconds
						
						// retry get
						if (result.getStatus().equals(StatusType.GET_ERROR)) {
							startTime = System.nanoTime();
							result = KVStore.get(randomKey);
							elapsedTime = System.nanoTime() - startTime; // elapsed time in nano seconds
						}
						
						double elapsedTimeGet = (double)elapsedTime / 1000000; // to ms
						
						getsSent++;
						if (result != null && (result.getStatus().equals(StatusType.GET_SUCCESS))) {
							perfLog.info("Get latency: " + elapsedTimeGet);
							getsSuccess++;
							
							String dataRec = result.getValue() + result.getKey();
							bitsSecond += dataRec.getBytes().length * 8;
							perfLog.info("GET BPS added: " + dataRec.getBytes().length * 8 + "for: " + dataRec);
						} else {
							getsFailed++;
							perfLog.info(result.getStatus()+" - Get latency: " + elapsedTimeGet);
						}
						
						evalInstance.getPerfInfo(this).update(elapsedTimePut, elapsedTimeGet);
					}
				}
				
				double LatencyGetAvg = Math.round(evalInstance.getPerfInfo(this).getLatencyGet());
				double LatencyPutAvg = Math.round(evalInstance.getPerfInfo(this).getLatencyPut());
				double bpsAvg = Math.round(evalInstance.getPerfInfo(this).getThroughpout());
				
				evalInstance.getLogger().info(this.name + " finished. " + putsSuccess + "/" + putsSent + "; " + getsSuccess + "/" + getsSent);
				evalInstance.getLogger().info("           Avg Get Latency: " + LatencyGetAvg + ", Avg Put Latency: " + LatencyPutAvg + ", Avg bps: " + bpsAvg);
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
