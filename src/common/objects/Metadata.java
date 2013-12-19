package common.objects;

import java.util.*;
import common.utilis.Hash;


/**
 * @author Dario
 * 
 * This class is used to represent the distributed system hash
 * ring and structure
 * 
 * The structure is held in a Sorted TreeMap (this.metadata) with as key, the hash
 * key of the server (calculated by hasing its address and IP) and as value
 * a ServerInfo object, containing the couple server ip
 *
 */
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
	
	/**
	 * We simply add the key, which will go in the right
	 * position of the tree since it's a SortedMap
	 * @param serverinfo to add
	 */
	
	public void add(ServerInfo serverinfo){
		String key = Hash.md5(serverinfo.toString());
		this.metadata.put(key, serverinfo);
	}
	
	/**
	 * @param serverinfo to remove, which will be hashed and then
	 * used as key to remove
	 */
	
	public void remove(ServerInfo serverinfo){
		String key = serverinfo.toHash();
		this.metadata.remove(key);
	}
	
	/**
	 * Returns the serverinfo responsible for handling the key
	 * We used tailMap which returns all the keys bigger than the given key,
	 * if the map is empty it means that there are no bigger keys (we are the biggest) so
	 * we take the first one (in the circular logic), otherwise we take the first
	 * key bigger than ours
	 * 
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
	  * Returns the predecessor server in the ring topology, using the same
	  * logic of get() but inversed
	  * @param key
	  * @return ServerInfo
	  */
	 public ServerInfo getPredecessor(String key) {
		 if (metadata.isEmpty()) {
		 	return null;
		 }
	     SortedMap<String, ServerInfo> headMap = metadata.headMap(key);
	     key = headMap.isEmpty() ? metadata.lastKey() : headMap.lastKey();
	   
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
