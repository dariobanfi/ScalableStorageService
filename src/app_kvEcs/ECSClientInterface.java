package app_kvEcs;

import java.io.*;

/**
 * 
 * @author ambi
 *
 * This is the interface for communicating with the
 * ECSServer, it is implemented by the ECSServer itself
 *
 */

public interface ECSClientInterface  {

	/**
	 * Launches n servers and sets their metadata and key
	 * @param numberOfNodes
	 * @return true if all was ok, false otherwise
	 * @throws IOException
	 */
	public boolean initService(int numberOfNodes) throws IOException;

	/**
	 * Start accepting client requests
	 * @throws IOException
	 */
	public void start() throws IOException;

	/**
	 * Stops accepting clients requests
	 * @throws IOException
	 */

	public void stop() throws IOException;


	/**
	 * Shuts down all the servers managed by the ECS
	 * @throws IOException
	 */
	public void shutDown() throws IOException;

	/**
	 * Adds a random node to the started servers
	 * @return true if all was ok, false otherwise
	 * @throws IOException
	 */
	public boolean addNode() throws IOException;

	/**
	 * Removes a random node from the server
	 * @return true if all ok, false otherwise
	 * @throws IOException
	 */
	public boolean removeNode() throws IOException;

}
