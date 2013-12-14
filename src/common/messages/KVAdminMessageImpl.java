package common.messages;

import java.util.ArrayList;
import java.util.List;
import common.objects.Metadata;
import common.objects.Range;
import common.objects.ServerInfo;

public class KVAdminMessageImpl implements KVAdminMessage {
	
	private KVAdminMessage.StatusType type;
	private Metadata metadata;
	private Range range;
	private ServerInfo serverinfo;



	private int number;
    private byte[] msgBytes;
    private static final byte END = 13;
    private static final byte SEPARATOR = 29;
    private static final String type_identifier = "statustype";
    private static final String metadata_identifier = "metadata";
    private static final String number_identifier = "number";
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
	
	public KVAdminMessageImpl(KVAdminMessage.StatusType statustype, int number){
        this.type = statustype;
        this.number = number;
        
        byte[] identifier0 = type_identifier.getBytes();
        byte[] identifier1 = number_identifier.getBytes();
        byte[] statusbytes = type.name().getBytes();
        byte[] numberbytes = String.valueOf(number).getBytes();
        
        byte[] tmp = new byte[identifier0.length + 1 +  statusbytes.length + 1 + identifier1.length + 1 + numberbytes.length + 1];
        
        System.arraycopy(identifier0, 0, tmp, 0, identifier0.length);
        tmp[identifier0.length] = SEPARATOR;
        
        System.arraycopy(statusbytes, 0, tmp, (identifier0.length + 1) , statusbytes.length);
        tmp[statusbytes.length + 1 + identifier0.length] = SEPARATOR;
        
        System.arraycopy(identifier1, 0, tmp, (identifier0.length + 1 + statusbytes.length + 1) , identifier1.length);
        tmp[identifier0.length + 1 + statusbytes.length + 1 + identifier1.length] = SEPARATOR;
        
        System.arraycopy(numberbytes, 0, tmp, (identifier0.length + 1 + statusbytes.length + 1 + identifier1.length + 1) , numberbytes.length);
        tmp[identifier0.length + 1 + statusbytes.length + 1 + identifier1.length + 1 + numberbytes.length] = END;
        
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
	
	
	public KVAdminMessageImpl(byte[] bytes){
		this.msgBytes = bytes;
		List<Byte> readelement =  new ArrayList<Byte>();
		String identifier = null;
		int binary_flag = 0;
		for(int i=0;i<bytes.length;i++){
			
			if(bytes[i]==SEPARATOR || bytes[i] == END){
				
				byte[] readelementarr = new byte[readelement.size()];
		        for (int j = 0; j < readelement.size(); j++){
		        	readelementarr[j] = readelement.get(j);
		        }
		        
		        if(binary_flag==0){
		        	identifier = new String(readelementarr);
//		        	System.out.println(identifier);
		        	readelement.clear();
		        	binary_flag = 1;
		        }
		        
		        else{
		        	
		        	if(identifier.equals(type_identifier)){
			           try{
			        	   System.out.println(identifier + " " + i + " " + new String(readelementarr));
			        	   System.out.println("---");
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
		        	
		        	else if(identifier.equals(serverinfo_identifier)){
		        		this.serverinfo = new ServerInfo(readelementarr);
		        		readelement.clear();
		        	}
		        	
		        	else if(identifier.equals(number_identifier)){
		        		this.number = Integer.parseInt(new String(readelementarr));
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
	
	/**
	 * @return the number
	 */
	public int getNumber() {
		return number;
	}
	




}
