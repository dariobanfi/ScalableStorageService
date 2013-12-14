package common.messages;

public interface Message {
	
    public enum PermissionType {
    	USER,
    	ADMIN
    }
	PermissionType getPermissionType();

}
