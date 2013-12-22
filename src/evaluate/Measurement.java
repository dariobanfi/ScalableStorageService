package evaluate;

public class Measurement {
	private double lPut = 0;
	private double lGet = 0;
	private int sPut = 0;
	private int sGet = 0;
	private int successPut = 0;
	private int successGet = 0;
	
	private double throughput = 0;
	
	int i = 0;
	int tCount = 0;
	
	public void updateThroughput (double throughputUpdate) {
		tCount++;
		throughput = throughput + ((throughputUpdate - throughput) / tCount);
	}
	
	public double getLatencyPut() {
		return this.lPut;
	}
	
	public double getLatencyGet() {
		return this.lGet;
	}
	
	public int getSuccessPut() {
		return this.successPut;
	}
	
	public int getSuccessGet() {
		return this.successGet;
	}
	
	public int getSentPut() {
		return this.sPut;
	}
	
	public int getSentGet() {
		return this.sGet;
	}
	
	public double getThroughput() {
		return this.throughput;
	}
	
	public void update(double latencyPutUpdate, double latencyGetUpdate, int PSent, int PSuccess, int GSent, int GSuccess) {
		i++;
		lPut = lPut + ((latencyPutUpdate - lPut)/i);
		lGet = lGet + ((latencyGetUpdate - lGet)/i);
		sPut = sPut + PSent;
		sGet = sGet + GSent;
		successPut = successPut + PSuccess;
		successGet = successGet + GSuccess;
	}
}
