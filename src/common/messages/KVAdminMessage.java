package common.messages;

import common.objects.Metadata;
import common.objects.Range;
import common.objects.ServerInfo;

public interface KVAdminMessage extends Message{
	
    public enum StatusType {
    	INIT_KV_SERVER,          
    	START,
    	STOP,
    	PUT,
    	SHUTDOWN,
    	LOCK_WRITE,
    	UNLOCK_WRITE,
    	MOVE_DATA,  		
    	UPDATE
    }

	/**
	 * @return the key that is associated with this message, 
	 * 		null if not key is associated.
	 */
	public StatusType getStatusType();
	
	public Metadata getMetadata();
	
	public Range getRange();
	
	public ServerInfo getServerInfo();
	
	public byte[] getBytes();

}
