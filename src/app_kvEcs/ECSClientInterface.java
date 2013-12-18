package app_kvEcs;

import java.io.*;


public interface ECSClientInterface  {

	public boolean initService(int numberOfNodes) throws IOException;

	public void start() throws IOException;


	public void stop() throws IOException;


	public void shutDown() throws IOException;


	public void addNode() throws IOException;

	public void removeNode() throws IOException;

}
