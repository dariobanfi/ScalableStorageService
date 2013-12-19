package common.objects;

import java.util.ArrayList;
import java.util.List;

import common.utilis.Hash;


/**
 * 
 * @author Dario
 * 
 * Object holding the information about a server address and port
 *
 */
public class ServerInfo {
	
	private String address;
	private int port;
	private static final byte COLUMN = 58;
	/**
	 * @return the address
	 */
	
	public ServerInfo(String address, int port){
		this.address = address;
		this.port = port;
	}
	
	public ServerInfo(byte[] bytes){
		List<Byte> readelement =  new ArrayList<Byte>();
        
        
		for(int i=0;i<bytes.length ;i++){
			
			if(bytes[i]==COLUMN){
				byte[] readelementbyte = new byte[readelement.size()];
		        for (int j = 0; j < readelement.size(); j++){
		        	readelementbyte[j] = readelement.get(j);
		        }
		        this.address = new String(readelementbyte);
		        readelement.clear();
			}
			else if(i == bytes.length - 1){
				readelement.add(bytes[i]);
				byte[] readelementbyte = new byte[readelement.size()];
		        for (int j = 0; j < readelement.size(); j++){
		        	readelementbyte[j] = readelement.get(j);
		        }
		        this.port = Integer.parseInt(new String(readelementbyte));
			}
			else{
				readelement.add(bytes[i]);
			}
		}
        
	}
	public String getAddress() {
		return address;
	}
	/**
	 * @param address the address to set
	 */
	public void setAddress(String address) {
		this.address = address;
	}
	/**
	 * @return the port
	 */
	public int getPort() {
		return port;
	}
	/**
	 * @param port the port to set
	 */
	public void setPort(int port) {
		this.port = port;
	}
	
	public String toString(){
		return this.address + ":" + this.port;
	}
	
	public String toHash(){
		return Hash.md5(toString());
	}
	public byte[] getBytes(){
		return (this.address + ":" + this.port).getBytes();
	}

	

}
