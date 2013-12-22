package testing;

import java.io.IOException;
import org.junit.Test;
import app_kvEcs.ECSServer;
import app_kvEcs.ECSServerKVLibrary;
import common.messages.KVAdminMessage;
import common.messages.KVMessage;
import common.objects.Metadata;
import common.objects.ServerInfo;
import communication.CommunicationModule;
import client.KVStore;
import junit.framework.TestCase;

/**
 * 
 * @author Dario
 * 
 * Please start the tests from the AllTests class, since the code
 * for the initialization of the distributed system is there
 *
 */
public class AdditionalTest extends TestCase {
	
	
	
	/**
	 * This test tries to send random bytes to the server to see if it doesn't crash
	 * and handles them correctly
	 * After that we try to connect with a KVStore to see if it's still on
	 */
   @Test
   	public void testKVServerUnknownMessage() {
	   KVStore kvClient = new KVStore("localhost", 50000);
       Exception ex = null;
       CommunicationModule c = new CommunicationModule("localhost", 50000);
       
       try{
           c.connect();
           byte[] request = "RAND0M5TR1NG0FH@CK3RCH@R5".getBytes();
           c.sendBytes(request);
           c.closeConnection();
       }
       catch(IOException exc){
               exc.printStackTrace();
       }
       
       kvClient = new KVStore("localhost", 50000);
       try {
           kvClient.connect();
           } catch (Exception e) {
               ex = e;
               e.printStackTrace();
           }
           
           
           assertNull(ex);
	}
	   
	
	/**
	 * This test starts two client connection to two different
	 * servers of the system. One will put a element and the other
	 * will try to read it.
	 * The client library will reconnect them to the right server and
	 * so they should be able to perform the operation
	 * 
	 * We assume here we have the servers on port 50000 and 500001 (since
	 * we started them in the AllTest class)
	 */
	@Test
	public void testMultipleClientInteraction() {
		
		Exception putex = null;
		Exception getex = null;
		KVMessage response = null;
		
		KVStore kvClient1 = new KVStore("localhost", 50000);
		KVStore kvClient2 = new KVStore("localhost", 50000);
		try {
			kvClient1.connect();
			kvClient2.connect();
		} catch (IOException e) {
		}
		
		try {
			kvClient1.put("test", "ciao");
		} catch (IOException e) {
			putex = e;
		}
		
		try {
			response = kvClient2.get("test");
		} catch (IOException e) {
			getex = e;
		}
		
		
		assertNull(putex);
		assertNull(getex);
		assertTrue(response.getValue().equals("ciao"));
	}
	
	
	/**
	 * This test checks if the dynamic data reallocation
	 * after removing a node works.
	 * To do this, we put in the distributed storage system all the letters
	 * all the alphabet as key and value combination.
	 * The letters will spread across the various servers, then we try to
	 * remove all the nodes except one, and check if that node returns
	 * a GET_SUCCESS for all letters we put.
	 */
	
	@Test
	public void testDataReallocation() {
		
		ECSServer ecs = AllTests.ecs;
		
		KVStore kv = new KVStore("localhost", 50000);
		try {
			kv.connect();
		} catch (IOException e) {
		}
		for(char i='a'; i<='z'; i++){
			try {
				kv.put(Character.toString(i), Character.toString(i));
			} catch (IOException e) {
			}
		}
		
		
		int servers = ecs.getStartedServers().size();
		
		// Removing nodes

		while(servers>1){
			ecs.removeNode();
			servers--;
		}
		
		try {
		    Thread.sleep(1000);
		} catch(InterruptedException ex) {
		    Thread.currentThread().interrupt();
		}
		
		// Trying to connect to the server still up 
		KVStore kv1 = new KVStore(ecs.getStartedServers().get(0).getAddress(), ecs.getStartedServers().get(0).getPort());

		try {
			kv1.connect();
		} catch (IOException e) {
		}
			
		// Trying to get back all the data we stored
		
		for(char i='a'; i<='z'; i++){
			try {
				System.out.println("Getting " + Character.toString(i) + " back");
				KVMessage msg = kv1.get(Character.toString(i));
				assertTrue(msg.getStatus().equals(KVMessage.StatusType.GET_SUCCESS) && msg.getValue().equals(Character.toString(i)));
			} catch (IOException e) {
			}
		}
	}
	
	/**
	 * This test proves that the consistent hashing implementation
	 * is working.
	 */
	
	@Test
	public void testConsistentHashing() {
		
		ServerInfo s0 = new ServerInfo("127.0.0.1", 50000);
		ServerInfo s1 = new ServerInfo("127.0.0.1", 50001);
		ServerInfo s2 = new ServerInfo("127.0.0.1", 50002);
		ServerInfo s3 = new ServerInfo("127.0.0.1", 50003);
		ServerInfo s4 = new ServerInfo("127.0.0.1", 50004);
		ServerInfo s5 = new ServerInfo("127.0.0.1", 50005);
		
		Metadata m = new Metadata();
		m.add(s0);
		m.add(s1);
		m.add(s2);
		m.add(s3);
		m.add(s4);
		m.add(s5);
		
		assertEquals(m.get("297e522da5461c774be1037dfb0a8226").toHash(), s5.toHash());
		assertEquals(m.get("358343938402ebb5110716c6e836f5a2").toHash(), s0.toHash());
		assertEquals(m.get("a98109598267087dfc364fae4cf24578").toHash(), s3.toHash());
		assertEquals(m.get("b3638a32c297f43aa37e63bbd839fc7e").toHash(), s2.toHash());
		assertEquals(m.get("da850509fc3b88a612b0bcad7a37963b").toHash(), s4.toHash());
		assertEquals(m.get("dcee0277eb13b76434e8dcd31a387709").toHash(), s1.toHash());
		
		// We try to get keys between ranges and check if they belong to the 
		// expected server
		
		assertEquals(m.get("33333333333333333333333333333333").toHash(), s0.toHash());
		assertEquals(m.get("ffffffffffffffffffffffffffffffff").toHash(), s5.toHash());

		// We remove s0 and s5 and get the same keys, which will be under the successor
		// responsibility
		
		m.remove(s0);
		m.remove(s5);
		
		assertEquals(m.get("33333333333333333333333333333333").toHash(), s3.toHash());
		assertEquals(m.get("ffffffffffffffffffffffffffffffff").toHash(), s3.toHash());
	}
	
	/**
	 * This test shows what happens when a client interacts with 
	 * a kVServer which is either in a STOPPED status or in a WRITE_LOCK
	 * status.
	 * In the first case, we shouldn't be able to communicate with the server,
	 * in the second, PUT operation must not have effect but get ones should work
	 */
	
	@Test
	public void testServerLocks() {
		
		ECSServer ecs = AllTests.ecs;
		
		ServerInfo server = ecs.getStartedServers().get(0);
		
		
		KVStore kv = new KVStore(server.getAddress(), server.getPort());

		try {
			kv.connect();
		} catch (IOException e) {
		}
		
	
		// Stopping the server
		
		ecs.stop();
		
		KVMessage response = null;
		try {
			response = kv.put("testingkey1", "testingvalue");
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		assertTrue(response!=null &&
				response.getStatus().equals(KVMessage.StatusType.SERVER_STOPPED));
		
		// Starting server
		
		ecs.start();
		
		// Setting the writelock to all servers
		
		ECSServerKVLibrary kvconnection = null;
			for(ServerInfo s: ecs.getStartedServers()){
			try {
				kvconnection = new ECSServerKVLibrary(s.getAddress(), s.getPort());
				KVAdminMessage adminresponse = kvconnection.lockWrite();
				if(!adminresponse.getStatusType().equals(KVAdminMessage.StatusType.SUCCESS))
					System.out.println("Error!");
			}
			catch (IOException e) {
				e.printStackTrace();
			}
		}
			

		
		response = null;
		try {
			response = kv.put("testingkey", "testingvalue");
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		System.out.println(new String(response.getBytes()));
		
		
		assertTrue(response.getStatus().equals(KVMessage.StatusType.SERVER_WRITE_LOCK));
		
		// Allowing write for  all servers
		
		kvconnection = null;
			for(ServerInfo s: ecs.getStartedServers()){
			try {
				kvconnection = new ECSServerKVLibrary(s.getAddress(), s.getPort());
				KVAdminMessage adminresponse = kvconnection.unlockWrite();
				if(!adminresponse.getStatusType().equals(KVAdminMessage.StatusType.SUCCESS))
					System.out.println("Error!");
			}
			catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		
		
	}
}
