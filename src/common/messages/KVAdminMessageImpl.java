package common.messages;

import java.util.ArrayList;
import java.util.List;


import common.objects.Metadata;
import common.objects.Range;
import common.objects.ServerInfo;


/**
 * 
 * @author Dario
 * 
 * This class provide message serialization and unserialization
 * The general format of a message is:
 * 
 * identifier SEPARATOR element SEPARATOR identifier2 SEPARATOR element2 END
 * 
 * For example a message with statustype = SUCCESS translates to:
 * 
 * statustype SEPARATOR SUCCESS END
 * 
 * Where separator and end are special byte characters
 *
 */
public class KVAdminMessageImpl implements KVAdminMessage {
	
	private KVAdminMessage.StatusType type;


	private Metadata metadata;
	private Range range;
	private ServerInfo serverinfo;


	private String key;
    private byte[] msgBytes;
    private static final byte END = 13;
    private static final byte SEPARATOR = 29;
    private static final String type_identifier = "statustype";
    private static final String metadata_identifier = "metadata";
    private static final String key_identifier = "key";
    private static final String range_identifier = "range";
    private static final String serverinfo_identifier = "serverinfo";
	
	public KVAdminMessageImpl(KVAdminMessage.StatusType statustype){
        this.type = statustype;
        byte[] identifier0 = type_identifier.getBytes();
        byte[] statusbytes = new String(type.name()).getBytes();
        byte[] tmp = new byte[identifier0.length + statusbytes.length + 2];
        System.arraycopy(identifier0, 0, tmp, 0, identifier0.length);
        tmp[identifier0.length] = SEPARATOR;
        System.arraycopy(statusbytes, 0, tmp, (identifier0.length + 1) , statusbytes.length);
        tmp[identifier0.length + 1 + statusbytes.length] = END;
        this.msgBytes = tmp;
	}
	
	
	public KVAdminMessageImpl(KVAdminMessage.StatusType statustype, Metadata metadata){
        this.type = statustype;
        this.metadata = metadata;
        
        byte[] identifier0 = type_identifier.getBytes();
        byte[] identifier1 = metadata_identifier.getBytes();
        byte[] statusbytes = type.name().getBytes();
        byte[] metadatabytes = metadata.getBytes();
        
        byte[] tmp = new byte[identifier0.length + 1 +  statusbytes.length + 1 + identifier1.length + 1 + metadatabytes.length + 1];
        
        System.arraycopy(identifier0, 0, tmp, 0, identifier0.length);
        tmp[identifier0.length] = SEPARATOR;
        
        System.arraycopy(statusbytes, 0, tmp, (identifier0.length + 1) , statusbytes.length);
        tmp[statusbytes.length + 1 + identifier0.length] = SEPARATOR;
        
        System.arraycopy(identifier1, 0, tmp, (identifier0.length + 1 + statusbytes.length + 1) , identifier1.length);
        tmp[identifier0.length + 1 + statusbytes.length + 1 + identifier1.length] = SEPARATOR;
        
        System.arraycopy(metadatabytes, 0, tmp, (identifier0.length + 1 + statusbytes.length + 1 + identifier1.length + 1) , metadatabytes.length);
        tmp[identifier0.length + 1 + statusbytes.length + 1 + identifier1.length + 1 + metadatabytes.length] = END;
        
        this.msgBytes = tmp;
	}

	
	public KVAdminMessageImpl(KVAdminMessage.StatusType statustype, Range range){
        this.type = statustype;
        this.range = range;
        
        byte[] identifier0 = type_identifier.getBytes();
        byte[] identifier1 = range_identifier.getBytes();
        byte[] statusbytes = type.name().getBytes();
        byte[] rangebytes = range.getBytes();
        
        byte[] tmp = new byte[identifier0.length + 1 +  statusbytes.length + 1 + identifier1.length + 1 + rangebytes.length + 1];
        
        System.arraycopy(identifier0, 0, tmp, 0, identifier0.length);
        tmp[identifier0.length] = SEPARATOR;
        
        System.arraycopy(statusbytes, 0, tmp, (identifier0.length + 1) , statusbytes.length);
        tmp[statusbytes.length + 1 + identifier0.length] = SEPARATOR;
        
        System.arraycopy(identifier1, 0, tmp, (identifier0.length + 1 + statusbytes.length + 1) , identifier1.length);
        tmp[identifier0.length + 1 + statusbytes.length + 1 + identifier1.length] = SEPARATOR;
        
        System.arraycopy(rangebytes, 0, tmp, (identifier0.length + 1 + statusbytes.length + 1 + identifier1.length + 1) , rangebytes.length);
        tmp[identifier0.length + 1 + statusbytes.length + 1 + identifier1.length + 1 + rangebytes.length] = END;
        
        this.msgBytes = tmp;
	}
	
	public KVAdminMessageImpl(KVAdminMessage.StatusType statustype, Range range, ServerInfo serverinfo){
        this.type = statustype;
        this.range = range;
        this.serverinfo = serverinfo;
        
        byte[] identifier0 = type_identifier.getBytes();
        byte[] statusbytes = type.name().getBytes();
        byte[] identifier1 = range_identifier.getBytes();
        byte[] rangebytes = range.getBytes();
        byte[] identifier2 = serverinfo_identifier.getBytes();
        byte[] serverinfobytes = serverinfo.getBytes();
        
        
        byte[] tmp = new byte[identifier0.length + 1 +  statusbytes.length + 1 +
                              identifier1.length + 1 + rangebytes.length + 1 + identifier2.length + 1
                              + serverinfobytes.length + 1];
        
        System.arraycopy(identifier0, 0, tmp, 0, identifier0.length);
        tmp[identifier0.length] = SEPARATOR;
        
        System.arraycopy(statusbytes, 0, tmp, (identifier0.length + 1) , statusbytes.length);
        tmp[statusbytes.length + 1 + identifier0.length] = SEPARATOR;
        
        System.arraycopy(identifier1, 0, tmp, (identifier0.length + 1 + statusbytes.length + 1) , identifier1.length);
        tmp[identifier0.length + 1 + statusbytes.length + 1 + identifier1.length] = SEPARATOR;
        
        System.arraycopy(rangebytes, 0, tmp, (identifier0.length + 1 + statusbytes.length + 1 + identifier1.length + 1) , rangebytes.length);
        tmp[identifier0.length + 1 + statusbytes.length + 1 + identifier1.length + 1 + rangebytes.length] = SEPARATOR;
        
        System.arraycopy(identifier2, 0, tmp, (identifier0.length + 1 + statusbytes.length + 1 + identifier1.length + 1 + rangebytes.length + 1) , identifier2.length);
        tmp[identifier0.length + 1 + statusbytes.length + 1 + identifier1.length  + 1 + rangebytes.length + 1 + identifier2.length] = SEPARATOR;
        
        System.arraycopy(serverinfobytes, 0, tmp, (identifier0.length + 1 + statusbytes.length + 1 + identifier1.length + 1 + rangebytes.length + 1 + identifier2.length + 1) , serverinfobytes.length);
        tmp[identifier0.length + 1 + statusbytes.length + 1 + identifier1.length  + 1 + rangebytes.length + 1 + identifier2.length + 1 + serverinfobytes.length] = END;
        
        this.msgBytes = tmp;
		
	}
	
	public KVAdminMessageImpl(KVAdminMessage.StatusType statustype, String key, Metadata metadata){
        this.type = statustype;
        this.key = key;
        this.metadata = metadata;
        
        byte[] identifier0 = type_identifier.getBytes();
        byte[] statusbytes = type.name().getBytes();
        byte[] identifier1 = key_identifier.getBytes();
        byte[] keybytes = key.getBytes();
        byte[] identifier2 = metadata_identifier.getBytes();
        byte[] metadatabytes = metadata.getBytes();
        
        
        byte[] tmp = new byte[identifier0.length + 1 +  statusbytes.length + 1 +
                              identifier1.length + 1 + keybytes.length + 1 + identifier2.length + 1
                              + metadatabytes.length + 1];
        
        System.arraycopy(identifier0, 0, tmp, 0, identifier0.length);
        tmp[identifier0.length] = SEPARATOR;
        
        System.arraycopy(statusbytes, 0, tmp, (identifier0.length + 1) , statusbytes.length);
        tmp[statusbytes.length + 1 + identifier0.length] = SEPARATOR;
        
        System.arraycopy(identifier1, 0, tmp, (identifier0.length + 1 + statusbytes.length + 1) , identifier1.length);
        tmp[identifier0.length + 1 + statusbytes.length + 1 + identifier1.length] = SEPARATOR;
        
        System.arraycopy(keybytes, 0, tmp, (identifier0.length + 1 + statusbytes.length + 1 + identifier1.length + 1) , keybytes.length);
        tmp[identifier0.length + 1 + statusbytes.length + 1 + identifier1.length + 1 + keybytes.length] = SEPARATOR;
        
        System.arraycopy(identifier2, 0, tmp, (identifier0.length + 1 + statusbytes.length + 1 + identifier1.length + 1 + keybytes.length + 1) , identifier2.length);
        tmp[identifier0.length + 1 + statusbytes.length + 1 + identifier1.length  + 1 + keybytes.length + 1 + identifier2.length] = SEPARATOR;
        
        System.arraycopy(metadatabytes, 0, tmp, (identifier0.length + 1 + statusbytes.length + 1 + identifier1.length + 1 + keybytes.length + 1 + identifier2.length + 1) , metadatabytes.length);
        tmp[identifier0.length + 1 + statusbytes.length + 1 + identifier1.length  + 1 + keybytes.length + 1 + identifier2.length + 1 + metadatabytes.length] = END;
        
        this.msgBytes = tmp;
		
	}
	
	/**
	 * Transforms a byte array into a KVMessage
	 * @param bytes
	 * @throws IllegalArgumentException
	 */
	public KVAdminMessageImpl(byte[] bytes) throws IllegalArgumentException{

		this.msgBytes = bytes;
		List<Byte> readelement =  new ArrayList<Byte>();
		String identifier = null;
		int binary_flag = 0;
		for(int i=0;i<bytes.length;i++){
			
			if(bytes[i]==SEPARATOR || i == bytes.length-1){
				
				if(i == bytes.length-1)
					readelement.add(bytes[i]);
				
				byte[] readelementarr = new byte[readelement.size()];
		        for (int j = 0; j < readelement.size(); j++){
		        	readelementarr[j] = readelement.get(j);
		        }
		        
		        if(binary_flag==0){
		        	identifier = new String(readelementarr);
		        	readelement.clear();
		        	binary_flag = 1;
		        }
		        
		        else{
		        	
		        	if(identifier.equals(type_identifier)){
			           try{
			        	   this.type = KVAdminMessage.StatusType.valueOf(new String(readelementarr));
			        	   readelement.clear();
		                }
		                catch(IllegalArgumentException e){
		                        throw new IllegalArgumentException("Malformed request");
		                }
		        	}
		        	else if(identifier.equals(metadata_identifier)){
		        		this.metadata = new Metadata(readelementarr);
		        		readelement.clear();
		        	}
		        	
		        	else if(identifier.equals(range_identifier)){
		        		this.range = new Range(readelementarr);
		        		readelement.clear();
		        	}
		        	
		        	else if(identifier.equals(key_identifier)){
		        		this.key = new String(readelementarr);
		        		readelement.clear();
		        	}
		        	
		        	else if(identifier.equals(serverinfo_identifier)){
		        		System.out.println(new String(readelementarr));
		        		this.serverinfo = new ServerInfo(readelementarr);
		        		readelement.clear();
		        	}
		        	
		        	else{
		        		throw new IllegalArgumentException("Serialization error");
		        	}
		        	binary_flag = 0;
				}
		        
			}
			else{
				readelement.add(bytes[i]);
			}
		}
	}


	@Override
	public StatusType getStatusType() {
		return this.type;
	}


	@Override
	public Metadata getMetadata() {
		return this.metadata;
	}


	@Override
	public Range getRange() {
		return this.range;
	}


	@Override
	public ServerInfo getServerInfo() {
		return this.serverinfo;
	}


	@Override
	public byte[] getBytes() {
		return this.msgBytes;
	}

	public String getKey() {
		return key;
	}


	public void setKey(String key) {
		this.key = key;
	}




}
