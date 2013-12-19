package common.objects;

import java.util.ArrayList;
import java.util.List;

/**
 * 
 * @author Dario
 * 
 * Object holding the two limits of a range
 *
 */
public class Range {
	
	private String lower_limit;
	private String upper_limit;
	private static final byte MINUS = 45;
	/**
	 * @return the lower_limit
	 */
	public Range(){
		
	}
	public Range(String lower_limit, String upper_limit){
		this.lower_limit = lower_limit;
		this.upper_limit = upper_limit;
	}
	
	public Range(byte[] bytes){
		int i=0;
		List<Byte> readlowerlimit =  new ArrayList<Byte>();
        List<Byte> readupperlimit = new ArrayList<Byte>();
        
        // Reading lowerlimit
		while(i<bytes.length){
            if(bytes[i]==MINUS){
                i++;
                break;
            }
            readlowerlimit.add(bytes[i]);
            i++;
		}
		byte[] tmpreadlowerlimit = new byte[readlowerlimit.size()];
        for (int j = 0; j < readlowerlimit.size(); j++){
        	tmpreadlowerlimit[j] = readlowerlimit.get(j);
        }
        this.lower_limit = new String(tmpreadlowerlimit);
		
        //Reading upperlimit
		while(i<bytes.length){
			readupperlimit.add(bytes[i]);
			i++;
		}
		byte[] tmpreadupperlimit = new byte[readupperlimit.size()];
        for (int j = 0; j < readupperlimit.size(); j++){
        	tmpreadupperlimit[j] = readupperlimit.get(j);
        }
        this.upper_limit = new String(tmpreadupperlimit);	
	}
	
	public String getLower_limit() {
		return lower_limit;
	}
	/**
	 * @param lower_limit the lower_limit to set
	 */
	public void setLower_limit(String lower_limit) {
		this.lower_limit = lower_limit;
	}
	/**
	 * @return the upper_limit
	 */
	public String getUpper_limit() {
		return upper_limit;
	}
	/**
	 * @param upper_limit the upper_limit to set
	 */
	public void setUpper_limit(String upper_limit) {
		this.upper_limit = upper_limit;
	}
	
	public byte[] getBytes(){
		return (lower_limit + "-" + upper_limit).getBytes();
	}
	

}
