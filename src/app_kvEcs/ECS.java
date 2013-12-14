package app_kvEcs;

import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.util.Map;

import org.apache.log4j.*;

import common.messages.*;


public interface ECS {
	
	public void initService(int numberOfNodes);
	
	public void start();
	
	public void stop();
	
	public void shutDown();
	
	public void addNode();
	
	public void removeNode();
	
}
