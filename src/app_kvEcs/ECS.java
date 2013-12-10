package app_kvEcs;

public interface ECS {

	
	public void initService(int numberOfNodes);
	
	public void start();
	
	public void stop();
	
	public void shutDown();
	
	public void addNode();
	
	public void removeNode();
	
}
