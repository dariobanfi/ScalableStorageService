package common.objects;

import java.util.ArrayList;
import java.util.List;

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
		int i=0;
		List<Byte> readserver =  new ArrayList<Byte>();
        List<Byte> readport = new ArrayList<Byte>();
        
        // Reading server address
		while(i<bytes.length){
            if(bytes[i]==COLUMN){
                i++;
                break;
            }
            readserver.add(bytes[i]);
            i++;
		}
		byte[] tmpreadserver = new byte[readserver.size()];
        for (int j = 0; j < readserver.size(); j++){
        	tmpreadserver[j] = readserver.get(j);
        }
        this.address = new String(tmpreadserver);
		
        //Reading port address
		while(i<bytes.length){
			readport.add(bytes[i]);
			i++;
		}
		byte[] tmpreadport = new byte[readport.size()];
        for (int j = 0; j < readport.size(); j++){
        	tmpreadport[j] = readport.get(j);
        }
        this.port = Integer.parseInt(new String(tmpreadport));

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
	public byte[] getBytes(){
		return (this.address + ":" + this.port).getBytes();
	}

	

}
