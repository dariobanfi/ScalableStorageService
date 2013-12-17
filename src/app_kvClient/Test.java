package app_kvClient;

import java.util.HashMap;
import java.util.Map;

import common.messages.*;
import common.objects.Metadata;
import common.objects.Range;
import common.objects.ServerInfo;
import common.utilis.Hash;

public class Test {

	public static void main(String[] args) {
		

		Range range = new Range("aaaa" , "ffff");
		KVAdminMessage k = new KVAdminMessageImpl(KVAdminMessage.StatusType.CLEANUP, range);
		
		System.out.println(new String(k.getBytes()));
//		Map<String,String> h = new HashMap<String,String>(); 
//		
//		h.put("000", "a");
//		h.put("111", "b");
//		h.put("222", "c");
//		h.put("ccc", "d");
//		
//		for (Map.Entry<String, String> entry : h.entrySet()) {
//			if(entry.getKey().compareTo("001")>0 && entry.getKey().compareTo("ccc")<=0 ){
//				System.out.print(entry.getKey());
//		    	System.out.println(entry.getValue());
//			}
//		}
//		ServerInfo s0 = new ServerInfo("127.0.0.1", 50000);
//		ServerInfo s1 = new ServerInfo("127.0.0.1", 50001);
//		ServerInfo s2 = new ServerInfo("127.0.0.1", 50002);
//		ServerInfo s3 = new ServerInfo("127.0.0.1", 50003);
//		
//		Metadata m = new Metadata();
//		m.add(s0);
//		m.add(s1);
//		m.add(s2);
//		m.add(s3);
//		
//		
//		System.out.println(m.toString());
//		System.out.println(m.getPredecessor("b98109598267087dfc364fae4cf24578").toString());
//		
//		KVMessage k = new KVMessageImpl(KVMessage.StatusType.PUT, "lol", "xd");
//		System.out.println(new String(k.getBytes()));
//		
//		
//		Message k1 = new Message(Message.PermissionType.USER, k.getBytes());
//
//		System.out.println(new String(k1.getMsgBytes()));
		
//		System.out.println(m.toString());
		
//		System.out.println(m.get("f08343938402ebb5110716c6e836f5a2").toString());
		
//		KVAdminMessage msg = new KVAdminMessageImpl(KVAdminMessage.StatusType.LOCK_WRITE, 5);
		
//		System.out.println(msg.getNumber());
		
//		KVAdminMessage msg = new KVAdminMessageImpl(KVAdminMessage.StatusType.LOCK_WRITE, new Range("11", "22"), new ServerInfo("111",222));
//		
//		byte[] b1 = msg.getBytes();
//		
//		System.out.println(new String(msg.getBytes()));
//		
//		KVAdminMessage msg1 = new KVAdminMessageImpl(b1);
//		
//		System.out.println(msg1.getStatusType());
//		System.out.println(msg1.getRange().getLower_limit());
//		System.out.println(msg1.getServerInfo().toString());

		
//		byte[] b = m.getBytes();
//		
//		System.out.println(new String(m.getBytes()));
//
//		
//		Metadata m1 = new Metadata(b);
		
//		System.out.println(new String(m1.getBytes()));

		
		
		

	}

}
