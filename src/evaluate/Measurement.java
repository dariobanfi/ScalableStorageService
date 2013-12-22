package evaluate;

public class Measurement {
	private double latencyPut = 0;
	private double latencyGet = 0;
	
	private double throughput = 0;
	
	int valueCount = 0;
	int throughputCount = 0;
	
	public void update(double latencyPutNew, double latencyGetNew) {
		valueCount++;
		latencyPut = latencyPut + ((latencyPutNew - latencyPut) / valueCount);
		latencyGet = latencyGet + ((latencyGetNew - latencyGet) / valueCount);
	}
	
	public void updateThroughput (double throughput) {
		throughputCount++;
		throughput = throughput + ((throughput - throughput) / throughputCount);
	}
	
	public double getLatencyPut() {
		return this.latencyPut;
	}
	
	public double getLatencyGet() {
		return this.latencyGet;
	}
	
	public double getThroughpout() {
		return this.throughput;
	}
}
