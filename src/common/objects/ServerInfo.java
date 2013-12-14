package common.objects;

public class ServerInfo {
	
	private String address;
	private int port;
	private Range range;
	/**
	 * @return the address
	 */
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
	/**
	 * @return the range
	 */
	public Range getRange() {
		return range;
	}
	/**
	 * @param range the range to set
	 */
	public void setRange(Range range) {
		this.range = range;
	}
	
	

}
