package common.messages;

import java.util.ArrayList;
import java.util.List;

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
        private static final byte END = 13;
        private static final byte SEPARATOR = 29;
        
        
        /**
         * @param type
         * Constructor for statustype-only messages ex: (GET_ERROR,PUT_SUCCESS)
         * Concatenates Statustype string bytes with endmessage
         */
        public KVMessageImpl(KVMessage.StatusType type){
                this.type = type;
                byte[] statusbytes = new String(type.name()).getBytes();
                byte[] tmp = new byte[statusbytes.length + 1];
                System.arraycopy(statusbytes, 0, tmp, 0, statusbytes.length);
                tmp[statusbytes.length] = END;
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
                byte[] statusbytes = type.name().getBytes();
                byte[] element = elem.getBytes();
                byte[] tmp = new byte[statusbytes.length + element.length + 2];
                System.arraycopy(statusbytes, 0, tmp, 0, statusbytes.length);
                tmp[statusbytes.length] = SEPARATOR;
                System.arraycopy(element, 0, tmp, (statusbytes.length + 1) , element.length);
                tmp[statusbytes.length + 1 + element.length] = END;
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
                byte[] statusbytes = type.name().getBytes();
                byte[] bkey = key.getBytes();
                byte[] bvalue = value.getBytes();
                byte[] tmp = new byte[statusbytes.length + bkey.length + bvalue.length + 3];
                System.arraycopy(statusbytes, 0, tmp, 0, statusbytes.length);
                tmp[statusbytes.length] = SEPARATOR;
                System.arraycopy(bkey, 0, tmp, statusbytes.length + 1, bkey.length );
                tmp[statusbytes.length + 1 + bkey.length] = SEPARATOR;
                System.arraycopy(bvalue, 0, tmp, statusbytes.length + 1 + bkey.length + 1, bvalue.length);
                tmp[statusbytes.length + 1 + bkey.length + 1 + bvalue.length] = END;
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
                
                // Using ArrayList for avoid dealing with fixed array sizes
                List<Byte> readstatus =  new ArrayList<Byte>();
                List<Byte> readkey = new ArrayList<Byte>();
                List<Byte> readmsg = new ArrayList<Byte>();        
                int i=0;
                boolean go_on = false;
                
                // Cycling through the whole message. If we find the separator we break out of the
                // loop and save what we read into the status array
                
                while(i<bytes.length){
                        if(bytes[i]==SEPARATOR){
                                i++;
                                go_on = true;
                                break;
                        }
                        readstatus.add(bytes[i]);
                        i++;
                }
                
                byte[] tmpreadstatus = new byte[readstatus.size()];
                byte[] tmpreadkey = null;
                byte[] tmpreadmsg = null;
                
                // Putting the ArrayList into a array for String conversion
            for (int j = 0; j < readstatus.size(); j++){
                    tmpreadstatus[j] = readstatus.get(j);
            }
            String statustypestring = new String(tmpreadstatus);
            
            // Since we found a separator, it means we have to go_on reading the next element,
            // in the same way as the upper while
                if (go_on){
                        go_on = false;
                        while(i<bytes.length){
                                if(bytes[i]==SEPARATOR){
                                        i++;
                                        go_on = true;
                                        break;
                                }
                                readkey.add(bytes[i]);
                                i++;
                        }
                        // Converting ArrayList into array
                        tmpreadkey = new byte[readkey.size()];
                    for (int j = 0; j < readkey.size(); j++){
                            tmpreadkey[j] = readkey.get(j);
                    }
                }
                
                // Reading last part of message (if present)
                if (go_on){
                        while(i<bytes.length){
                                readmsg.add(bytes[i]);
                                i++;
                        }
                        tmpreadmsg = new byte[readmsg.size()];
                    for (int j = 0; j < readmsg.size(); j++){
                            tmpreadmsg[j] = readmsg.get(j);
                    }
                }
                KVMessage.StatusType finalstatus = null;
                
                // Checking if the message belongs to the messages enum
                try{
                        finalstatus = KVMessage.StatusType.valueOf(statustypestring);
                }
                catch(IllegalArgumentException e){
                        throw new IllegalArgumentException("Malformed request");
                }
                
                this.type = finalstatus;
                
                
                // Setting the values unmarshaled in the message 
                
                if(tmpreadmsg!=null && tmpreadkey!=null){
                        this.value = new String(tmpreadmsg);
                        this.key = new String(tmpreadkey);
                }
                else if(tmpreadkey!=null){
                        this.key = new String(tmpreadkey);
                        this.value = new String(tmpreadkey);
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
        

}