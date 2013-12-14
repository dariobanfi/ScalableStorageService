package common.messages;

import common.objects.Metadata;
import common.objects.Range;
import common.objects.ServerInfo;

public class KVAdminMessageImpl implements KVAdminMessage {
	
	private KVAdminMessage.StatusType type;
	private Metadata metadata;
	private Range range;
	private ServerInfo serverinfo;
    private byte[] msgBytes;
    private static final byte END = 13;
    private static final byte SEPARATOR = 29;
    private static final String type_identifier = "statustype";
    private static final String metadata_identifier = "metadata";
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
	
	public KVAdminMessageImpl(KVAdminMessage.StatusType statustype, Range range, ServerInfo serverinfo){
        this.type = statustype;
        this.range = range;
        this.serverinfo = serverinfo;
        
        byte[] identifier0 = type_identifier.getBytes();
        byte[] statusbytes = type.name().getBytes();
        byte[] identifier1 = metadata_identifier.getBytes();
        byte[] rangebytes = range.getBytes();
        byte[] identifier2 = serverinfo_identifier.getBytes();
        byte[] serverinfobytes = serverinfo.getBytes();
        
        
        byte[] tmp = new byte[identifier0.length + 1 +  statusbytes.length + 1 +
                              identifier1.length + 1 + rangebytes.length + 1
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
        tmp[identifier0.length + 1 + statusbytes.length + 1 + identifier1.length  + 1 + rangebytes.length + identifier2.length] = SEPARATOR;
        
        System.arraycopy(serverinfobytes, 0, tmp, (identifier0.length + 1 + statusbytes.length + 1 + identifier1.length + 1 + rangebytes.length + 1 + identifier2.length + 1) , serverinfobytes.length);
        tmp[identifier0.length + 1 + statusbytes.length + 1 + identifier1.length  + 1 + rangebytes.length + identifier2.length + 1 + serverinfobytes.length] = END;
        
        this.msgBytes = tmp;
		
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

}
