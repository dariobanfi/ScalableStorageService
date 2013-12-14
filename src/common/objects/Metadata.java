package common.objects;

import java.nio.charset.Charset;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

public class Metadata {

	private Map<String,ServerInfo> metadata;
	private final byte SEMICOLON = 59;
	
	public Metadata(){
		this.metadata = new TreeMap<String,ServerInfo>();
	}
	
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
		String string_serverinfo = serverinfo.getAddress() + ":" + serverinfo.getPort();
        MessageDigest messageDigest = null;
		try {
			messageDigest = MessageDigest.getInstance("MD5");
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
        messageDigest.reset();
        messageDigest.update(string_serverinfo.getBytes(Charset.forName("UTF8")));
        final byte[] resultByte = messageDigest.digest();
        
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < resultByte.length; i++) {
          sb.append(Integer.toString((resultByte[i] & 0xff) + 0x100, 16).substring(1));
        }
		String key = sb.toString();
		this.metadata.put(key, serverinfo);
	}
	
	public void remove(ServerInfo serverinfo){
		String string_serverinfo = serverinfo.getAddress() + ":" + serverinfo.getPort();
        MessageDigest messageDigest = null;
		try {
			messageDigest = MessageDigest.getInstance("MD5");
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
        messageDigest.reset();
        messageDigest.update(string_serverinfo.getBytes(Charset.forName("UTF8")));
        final byte[] resultByte = messageDigest.digest();
        
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < resultByte.length; i++) {
          sb.append(Integer.toString((resultByte[i] & 0xff) + 0x100, 16).substring(1));
        }
		String key = sb.toString();
		this.metadata.remove(key);
		
	}
	
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
	
}
