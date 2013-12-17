package common.utilis;

import java.nio.charset.Charset;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class Hash {

	public static String md5(String input){
        MessageDigest messageDigest = null;
		try {
			messageDigest = MessageDigest.getInstance("MD5");
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
        messageDigest.reset();
        messageDigest.update(input.getBytes(Charset.forName("UTF8")));
        final byte[] resultByte = messageDigest.digest();
        
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < resultByte.length; i++) {
          sb.append(Integer.toString((resultByte[i] & 0xff) + 0x100, 16).substring(1));
        }
		String key = sb.toString();
		return key;
	}
}
