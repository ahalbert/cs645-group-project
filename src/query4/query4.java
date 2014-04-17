package query4;

public class query4 {

	static String queries = "10k-sample-queries4.txt";
	static String dataDir = "data10k/";
	
	public static void main(String[] args) {
		System.out.println("Testing query 4 on 10k data." + System.getProperty("user.dir"));
		MostCentralPeople mcp = new MostCentralPeople(dataDir, queries);
		mcp.naiveApproach();
	}
	

}
