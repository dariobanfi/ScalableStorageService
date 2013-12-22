package app_kvClient;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import client.KVStore;
import app_kvEcs.ECSClient;
import app_kvEcs.ECSServer;
import app_kvEcs.ECSServerKVLibrary;
import common.messages.*;
import common.objects.Metadata;
import common.objects.Range;
import common.objects.ServerInfo;
import common.utilis.Hash;

public class Test {

	public static Logger logger = Logger.getLogger("Test>");
	
	public static void main(String[] args) throws UnknownHostException, IOException {
		


		// ECS
		
	
		
//		ECSServer ecs = new ECSServer("settings.config");
//		ECSClient ecsclient = new ECSClient(ecs);
//		
//		ecs.initService(4);
//		
//		ecs.start();
		
//		ServerInfo s0 = new ServerInfo("127.0.0.1", 50000);	
//		Metadata m = new Metadata();
//		m.add(s0);
//		ECSServerKVLibrary e = new ECSServerKVLibrary("127.0.0.1", 50000);
//		e.initKVServer(m);
//	
//		KVAdminMessage response = e.start();
//		
//		response = e.unlockWrite();
		
		KVStore kv = new KVStore("127.0.0.1", 50002);
		kv.connect();
        for(char i='a';i<'z';i++)
        	kv.put(Character.toString(i), Character.toString(i));
//		KVMessage resp = kv.get("a");
//		System.out.println(new String(resp.getBytes()));
//		System.out.println(response.getStatusType());
//			ServerInfo i = new ServerInfo("127.0.0.1", 5000);
//			ProcessBuilder pb = new ProcessBuilder("ssh", "-n", i.getAddress(), "nohup", "java -jar", 
//					"~/Destop/Programming/Eclipse/ScalableStorageService/ms3-server.jar", ""+i.getPort(), "&");
//			pb.redirectErrorStream();
//			try {
//				pb.start();
//			} catch (IOException e) {
//			}
	    

//		Range range = new Range("aaaa" , "ffff");
//		ServerInfo s = new ServerInfo("aaa", 5000);
//		KVAdminMessage k = new KVAdminMessageImpl(KVAdminMessage.StatusType.MOVE_DATA, range, s);
//		
//		System.out.println(new String(k.getBytes()));
//		for(byte b:k.getBytes())
//			System.out.print(b + " ");
//		
//		KVAdminMessage k2 = new KVAdminMessageImpl(k.getBytes());
//		System.out.println(k2.getRange().getLower_limit());
//		System.out.println(k2.getRange().getUpper_limit());

//		

//
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
//		System.out.println(m.toString());
//		
//		String dropServerKey = s2.toHash();
//		String dropServerPredecessorKey = m.getPredecessor(dropServerKey).toHash();
//		System.out.println(dropServerKey);
//		System.out.println(dropServerPredecessorKey);
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
