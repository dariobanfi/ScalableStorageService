package testing;

import java.io.IOException;

import org.apache.log4j.Level;

import app_kvEcs.ECSServer;
import junit.extensions.TestSetup;
import junit.framework.Test;
import junit.framework.TestSuite;
import logger.LogSetup;

public class AllTests {

	public static ECSServer ecs;
	
	static {
		try {
			new LogSetup("logs/testing/test.log", Level.ALL);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	
	
	public static Test suite() {
		TestSuite clientSuite = new TestSuite("ScalableStorage ServerTest-Suite");
		clientSuite.addTestSuite(ConnectionTest.class);
		clientSuite.addTestSuite(InteractionTest.class); 
		clientSuite.addTestSuite(AdditionalTest.class); 
		TestSetup setup = new TestSetup(clientSuite){
            protected void setUp(  ) throws Exception {
            	// We start with a very high number which will translate
            	// to all the servers in the settings.config file
    			ecs = new ECSServer("settings.config");
    			ecs.initService(10000);
    			ecs.start();
            }

            protected void tearDown(  ) throws Exception {
                ecs.shutDown();
            }
        };
		
		return setup;
	}
	
}
