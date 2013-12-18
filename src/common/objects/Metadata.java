package common.objects;

import java.util.*;
import common.utilis.Hash;

public class Metadata {

	private SortedMap<String, ServerInfo> metadata;
	private final byte SEMICOLON = 59;
	
	public Metadata(){
		this.metadata = new TreeMap<String ,ServerInfo>();
	}
	
	/**
	 * Unserialization constructor, takes a byte and creates
	 * a TreeMap sctructure with the hash as key, and the serverinfo
	 * as element
	 * @param bytes
	 */
	public Metadata(byte[] bytes){
		this.metadata = new TreeMap<String,ServerInfo>();
		List<Byte> readserverinfobytes =  new ArrayList<Byte>();
		for(int i=0;i<bytes.length;i++){
			
			if(bytes[i]==SEMICOLON){
				byte[] readstatustype = new byte[readserverinfobytes.size()];
		        for (int j = 0; j < readserverinfobytes.size(); j++){
		        	readstatustype[j] = readserverinfobytes.get(j);
		        }
				add(new ServerInfo(readstatustype));
				readserverinfobytes = new ArrayList<Byte>();
			}
			else{
				readserverinfobytes.add(bytes[i]);
			}
		}
	}
	
	public void add(ServerInfo serverinfo){
		String key = Hash.md5(serverinfo.toString());
		this.metadata.put(key, serverinfo);
	}
	
	public void remove(ServerInfo serverinfo){
		String key = Hash.md5(serverinfo.toString());
		this.metadata.put(key, serverinfo);
		this.metadata.remove(key);
		
	}
	
	/**
	 * Returns the serverinfo responsible for handing the key
	 * @param key
	 * @return ServerInfo
	 */
	 public ServerInfo get(String key) {
		   if (metadata.isEmpty()) {
		     return null;
		   }
		   if (!metadata.containsKey(key)) {
			     SortedMap<String, ServerInfo> tailMap = metadata.tailMap(key);
			     key = tailMap.isEmpty() ? metadata.firstKey() : tailMap.firstKey();
		   }
		   return metadata.get(key);
		 }
	 
	 /**
	  * Returns the predecessor server in the ring topology
	  * @param key
	  * @return ServerInfo
	  */
	 public ServerInfo getPredecessor(String key) {
		   if (metadata.isEmpty()) {
		     return null;
		   }
		   if (!metadata.containsKey(key)) {
			     SortedMap<String, ServerInfo> headMap = metadata.headMap(key);
			     key = headMap.isEmpty() ? metadata.lastKey() : headMap.lastKey();
		   }
		   return metadata.get(key);
		 }
	 
	 /**
	  * Serialization function for sending over the network
	  * @return a byte representation array
	  */
		public byte[] getBytes(){
			List<Byte> metadatalist =  new ArrayList<Byte>();
			for(Map.Entry<String,ServerInfo> entry : metadata.entrySet()) {
				  ServerInfo serverinfo = entry.getValue();
				  
				  byte[] representation = serverinfo.getBytes();
				  for(byte b:representation){
					  metadatalist.add(b);
				  }
				  metadatalist.add(SEMICOLON);
			}
			
			byte[] returnbytes = new byte[metadatalist.size()];
	        for (int j = 0; j < metadatalist.size(); j++){
	        	returnbytes[j] = metadatalist.get(j);
	        }
	        
	        return returnbytes;
		}
		
		/**
		 * toString representation of the metadata tree, useful for debugging
		 */
		
		public String toString(){
			String retvalue = "";
			for(Map.Entry<String,ServerInfo> entry : metadata.entrySet()) {
				  ServerInfo serverinfo = entry.getValue();
				  String key = entry.getKey();
				  retvalue =retvalue + "|" + key + ";" + new String(serverinfo.getBytes());
			}
			
	        return retvalue;
		}
	
}
