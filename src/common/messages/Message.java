package common.messages;

public class Message {
	
    public enum PermissionType {
    	USER,
    	ADMIN
    }
    
    private byte[] payload;
    private PermissionType permission;
    
    public Message(byte[] bytes){
    	
    }
    
    public Message(PermissionType p, byte[] payload){
    	
    }

}
