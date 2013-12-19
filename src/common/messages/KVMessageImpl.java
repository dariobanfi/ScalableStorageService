package common.messages;

import java.util.ArrayList;
import java.util.List;

import common.objects.Metadata;

/**
 * @author Dario
 * 
 * This class encapsulates the most important functions of the protocol, the message marshalling/unmarshalling
 * The KVMessageImpl has different constructors for different types of messages and a costructor
 * to decode bytes into KVMessage
 * The message is sent over the socket as byte stream, so some special characters are employed to 
 * to separate messages: 29 as separator between statustype, key and value and 13 as message
 * delimiter
 *
 */
public class KVMessageImpl implements KVMessage {
        
        private KVMessage.StatusType type;
        private String key;
        private String value;
        private byte[] msgBytes;
		private Metadata metadata;
        private static final byte END = 13;
        private static final byte SEPARATOR = 29;
        private static final String type_identifier = "statustype";
        private static final String metadata_identifier = "metadata";
        private static final String key_identifier = "key";
        private static final String value_identifier = "value";
        
        
        /**
         * @param type
         * Constructor for statustype-only messages ex: (GET_ERROR,PUT_SUCCESS)
         * Concatenates Statustype string bytes with endmessage
         */
        public KVMessageImpl(KVMessage.StatusType type){
            this.type = type;
            byte[] identifier0 = type_identifier.getBytes();
            byte[] statusbytes = new String(type.name()).getBytes();
            byte[] tmp = new byte[identifier0.length + statusbytes.length + 2];
            System.arraycopy(identifier0, 0, tmp, 0, identifier0.length);
            tmp[identifier0.length] = SEPARATOR;
            System.arraycopy(statusbytes, 0, tmp, (identifier0.length + 1) , statusbytes.length);
            tmp[identifier0.length + 1 + statusbytes.length] = END;
            this.msgBytes = tmp;
        }
        
        /**
         * @param type
         * @param elem
         * 
         * Constructor for 2 elements messages (GET <key>, GET_SUCCESS <value>)
         * Concatenates string of statustype + separator + key/value + endmessage
         */
        public KVMessageImpl(KVMessage.StatusType type, String elem){
            this.type = type;
            
            byte[] identifier0 = type_identifier.getBytes();
            byte[] identifier1 = null;
            if(type.equals(KVMessage.StatusType.GET))
            	identifier1 = key_identifier.getBytes();
            else if(type.equals(KVMessage.StatusType.GET_SUCCESS) || type.equals(KVMessage.StatusType.PUT_UPDATE))
            	identifier1 = value_identifier.getBytes();
            else
            	throw new IllegalArgumentException("Invalid type");
            
            byte[] statusbytes = type.name().getBytes();
            byte[] elembytes = elem.getBytes();
            
            byte[] tmp = new byte[identifier0.length + 1 +  statusbytes.length + 1 + identifier1.length + 1 + elembytes.length + 1];
            
            System.arraycopy(identifier0, 0, tmp, 0, identifier0.length);
            tmp[identifier0.length] = SEPARATOR;
            
            System.arraycopy(statusbytes, 0, tmp, (identifier0.length + 1) , statusbytes.length);
            tmp[statusbytes.length + 1 + identifier0.length] = SEPARATOR;
            
            System.arraycopy(identifier1, 0, tmp, (identifier0.length + 1 + statusbytes.length + 1) , identifier1.length);
            tmp[identifier0.length + 1 + statusbytes.length + 1 + identifier1.length] = SEPARATOR;
            
            System.arraycopy(elembytes, 0, tmp, (identifier0.length + 1 + statusbytes.length + 1 + identifier1.length + 1) , elembytes.length);
            tmp[identifier0.length + 1 + statusbytes.length + 1 + identifier1.length + 1 + elembytes.length] = END;
            
            this.msgBytes = tmp;
        }
        
        /**
         * @param type
         * @param key
         * @param value
         * 
         * It creates the message using separators like the others
         * This constructor is used for the PUT requests
         * 
         */
        
        public KVMessageImpl(KVMessage.StatusType type, String key, String value){
            this.type = type;
            
            byte[] identifier0 = type_identifier.getBytes();
            byte[] identifier1 = key_identifier.getBytes();
            byte[] identifier2 = value_identifier.getBytes();
            byte[] statusbytes = type.name().getBytes();
            byte[] keybytes = key.getBytes();
            byte[] elembytes = value.getBytes();
            
            byte[] tmp = new byte[identifier0.length + 1 +
                                  statusbytes.length + 1 +
                                  identifier1.length + 1 +
                                  keybytes.length + 1 +
                                  identifier2.length + 1 +
                                  elembytes.length + 1];
            
            System.arraycopy(identifier0, 0, tmp, 0, identifier0.length);
            tmp[identifier0.length] = SEPARATOR;
            
            System.arraycopy(statusbytes, 0, tmp, (identifier0.length + 1) , statusbytes.length);
            tmp[statusbytes.length + 1 + identifier0.length] = SEPARATOR;
            
            System.arraycopy(identifier1, 0, tmp, (identifier0.length + 1 + statusbytes.length + 1) , identifier1.length);
            tmp[identifier0.length + 1 + statusbytes.length + 1 + identifier1.length] = SEPARATOR;
            
            System.arraycopy(keybytes, 0, tmp, (identifier0.length + 1 + statusbytes.length + 1 + identifier1.length + 1) , keybytes.length);
            tmp[identifier0.length + 1 + statusbytes.length + 1 + identifier1.length + 1 + keybytes.length] = SEPARATOR;
            
            System.arraycopy(identifier2, 0, tmp, (identifier0.length + 1 + statusbytes.length + 1 + identifier1.length + 1 + keybytes.length + 1) , identifier2.length);
            tmp[identifier0.length + 1 + statusbytes.length + 1 + identifier1.length + 1 + keybytes.length + 1 + identifier2.length] = SEPARATOR;
            
            System.arraycopy(elembytes, 0, tmp, (identifier0.length + 1 + statusbytes.length + 1 + identifier1.length + 1 + keybytes.length + 1 + identifier2.length + 1) , elembytes.length);
            tmp[identifier0.length + 1 + statusbytes.length + 1 + identifier1.length + 1 + keybytes.length + 1 + identifier2.length + 1 + elembytes.length] = END;
            
            this.msgBytes = tmp;
        }
        
    	public KVMessageImpl(KVMessage.StatusType statustype, Metadata metadata){
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
        
        /**
         * @param bytes
         * @throws IllegalArgumentException
         * 
         * Unmarshals a byte array into a KVMessage
         * Throws IllegalArgumentException if message is not understood
         */
        public KVMessageImpl(byte[] bytes) throws IllegalArgumentException{
                
    		this.msgBytes = bytes;
    		List<Byte> readelement =  new ArrayList<Byte>();
    		String identifier = null;
    		int binary_flag = 0;
    		for(int i=0;i<bytes.length;i++){
    			
    			if(bytes[i]==SEPARATOR ||  i == bytes.length-1){
    				
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
    			        	   this.type = KVMessage.StatusType.valueOf(new String(readelementarr));
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
    		        	
    		        	else if(identifier.equals(key_identifier)){
    		        		this.key = new String(readelementarr);
    		        		readelement.clear();
    		        	}
    		        	
    		        	else if(identifier.equals(value_identifier)){
    		        		this.value = new String(readelementarr);
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
        public String getKey() {
                return key;
        }

        @Override
        public String getValue() {
                return value;
        }

        @Override
        public StatusType getStatus() {
                return type;
        }

        @Override
        public byte[] getBytes() {
                return msgBytes;
        }

		@Override
		public Metadata getMetaData() {
			return this.metadata;
		}
        

}