package common.messages;

import common.objects.Metadata;
import common.objects.Range;
import common.objects.ServerInfo;

public interface KVAdminMessage {
	
    public enum StatusType {
    	INIT_SERVICE,
    	INIT_KV_SERVER,          
    	START,
    	STOP,
    	PUT,
    	SHUTDOWN,
    	LOCK_WRITE,
    	UNLOCK_WRITE,
    	MOVE_DATA,  		
    	UPDATE,
    	ADD_NODE,
    	REMOVE_NODE,
    	SUCCESS,
    	FAILURE
    }

	/**
	 * @return the key that is associated with this message, 
	 * 		null if not key is associated.
	 */
	public StatusType getStatusType();
	
	public Metadata getMetadata();
	
	public Range getRange();
	
	public int getNumber();
	
	public ServerInfo getServerInfo();
	
	public byte[] getBytes();

}
