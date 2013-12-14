package common.messages;

import common.objects.Metadata;

public interface KVAdminMessage {
	
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
	
	
	/**
	 * @return the value that is associated with this message, 
	 * 		null if not value is associated.
	 */
	public Metadata getMetadata();

}
