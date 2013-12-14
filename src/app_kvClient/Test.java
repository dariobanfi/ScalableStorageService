package app_kvClient;

import common.messages.*;
import common.objects.Metadata;
import common.objects.ServerInfo;

public class Test {

	public static void main(String[] args) {

		ServerInfo s0 = new ServerInfo("127.0.0.1", 50000);
		ServerInfo s1 = new ServerInfo("127.0.0.1", 50001);
		ServerInfo s2 = new ServerInfo("127.0.0.1", 50002);
		ServerInfo s3 = new ServerInfo("127.0.0.1", 50003);
		
		Metadata m = new Metadata();
		m.add(s0);
		m.add(s1);
		m.add(s2);
		m.add(s3);
		
		KVAdminMessage msg = new KVAdminMessageImpl(KVAdminMessage.StatusType.LOCK_WRITE);
		
		byte[] b1 = msg.getBytes();
		
		System.out.println(new String(msg.getBytes()));
		
		KVAdminMessage msg1 = new KVAdminMessageImpl(b1);
		
		System.out.println(new String(msg1.getBytes()));
		
//		byte[] b = m.getBytes();
//		
//		System.out.println(new String(m.getBytes()));
//
//		
//		Metadata m1 = new Metadata(b);
//		
//		System.out.println(new String(m1.getBytes()));

		
		
		

	}

}
