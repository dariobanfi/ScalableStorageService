package common.messages;

import common.objects.Metadata;
import common.objects.Range;
import common.objects.ServerInfo;

/**
 * Thsis interface handles the messages sent between KVServer and ECSServer
 * @author Dario
 *
 */
public interface KVAdminMessage {

	
    public enum StatusType {
    	INIT_KV_SERVER,       
    	START,
    	STOP,
    	SHUTDOWN,
    	LOCK_WRITE,
    	UNLOCK_WRITE,
    	MOVE_DATA,  		
    	UPDATE,
    	SUCCESS,
    	CLEANUP
    }
    
    /**
     * 
     * @return the statustype of the msg
     */
	public StatusType getStatusType();
	
	/**
	 * @return metadata  inside the message
	 */
	public Metadata getMetadata();
	
	/** Range inside the message
	 * @return
	 */
	public Range getRange();
	
	/**
	 * @return key inside the message
	 */
	public String getKey();
	
	/**
	 * @return serverinfo inside the message
	 */
	
	public ServerInfo getServerInfo();

	/**
	 * @return the byte representation of the msg
	 */
	public byte[] getBytes();

}
