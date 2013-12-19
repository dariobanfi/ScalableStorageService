package common.messages;

/**
 * 
 * @author Dario
 * 
 * This is basically a wrap class which is used by the KVServer to differentiate
 * between user and admin messages.
 * A single byte is added in front of a byte payload (the KVMessage or KVAdminMessage representation),
 * and with this we can see the message permissions
 *
 */

public class Message {
	
    public enum PermissionType {
    	USER,
    	ADMIN
    }
    
    private byte[] payload;
	private PermissionType permission;
    private byte[] msgBytes;
    private byte admin_flag = 1;
    private byte user_flag  = 2;
    
    /**
     * Marshals the byte array into a msg
     * @param bytes to serialize
     */
    public Message(byte[] bytes){
    	this.msgBytes = bytes;
    	if(bytes[0]== admin_flag){
    		byte[] payload = new byte[bytes.length-1];
    		System.arraycopy(bytes, 1, payload,  0, bytes.length-1);
    		this.permission = PermissionType.ADMIN;
    		this.payload = payload;
    	}
    	else if(bytes[0] == user_flag){
    		byte[] payload = new byte[bytes.length-1];
    		System.arraycopy(bytes, 1, payload,  0, bytes.length-1);
    		this.permission = PermissionType.USER;    	
    		this.payload = payload;
    	}
    	else{
    		throw new IllegalArgumentException("Malformed message");
    	}
    }
    
    public Message(PermissionType p, byte[] payload){
    	
    	if(p.equals(PermissionType.ADMIN)){
    		byte[] bytes = new byte[payload.length+1];
    		bytes[0] = admin_flag;
    		System.arraycopy(payload, 0, bytes,  1, payload.length);
    		this.permission = p;
    		this.payload = payload;
    		this.msgBytes = bytes; 
    	}
    	
    	else if(p.equals(PermissionType.USER)){
    		byte[] bytes = new byte[payload.length+1];
    		bytes[0] = user_flag;
    		System.arraycopy(payload, 0, bytes,  1, payload.length);
    		this.permission = p;
    		this.payload = payload;
    		this.msgBytes = bytes;
    	}
    	else{
    		throw new IllegalArgumentException("Malformed message");
    	} 	
    }
    
    
	public byte[] getPayload() {
		return payload;
	}

	/**
	 * @param payload the payload to set
	 */
	public void setPayload(byte[] payload) {
		this.payload = payload;
	}

	/**
	 * @return the permission
	 */
	public PermissionType getPermission() {
		return permission;
	}

	/**
	 * @param permission the permission to set
	 */
	public void setPermission(PermissionType permission) {
		this.permission = permission;
	}

	/**
	 * @return the msgBytes
	 */
	public byte[] getBytes() {
		return this.msgBytes;
	}

	/**
	 * @param msgBytes the msgBytes to set
	 */
	public void setMsgBytes(byte[] msgBytes) {
		this.msgBytes = msgBytes;
	}

}
