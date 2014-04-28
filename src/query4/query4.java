package query4;

public class query4 {

	static String queries = "1k-query4.txt";
	static String dataDir = "data1k/";
	
	public static void main(String[] args) {
		System.out.println("Testing query 4 on 1k data." + System.getProperty("user.dir"));
		MostCentralPeople mcp = new MostCentralPeople(dataDir, queries);
		mcp.naiveApproach();
	}
	

}
