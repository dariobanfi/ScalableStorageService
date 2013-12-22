package evaluate;

public class Measurement {
	private double lPut = 0;
	private double lGet = 0;
	
	private double throughput = 0;
	
	int i = 0;
	int tCount = 0;
	
	public void updateThroughput (double throughput) {
		tCount++;
		throughput = throughput + ((throughput - throughput) / tCount);
	}
	
	public double getLatencyPut() {
		return this.lPut;
	}
	
	public double getLatencyGet() {
		return this.lGet;
	}
	
	public double getThroughput() {
		return this.throughput;
	}
	
	public void update(double latencyPut, double latencyGet) {
		i++;
		lPut = lPut + ((latencyPut - latencyPut)/i);
		lGet = lGet + ((latencyGet - latencyGet)/i);
	}
}
