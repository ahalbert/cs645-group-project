package query3;


public class query3 {

	static String queries = "1k-query3.txt";
	static String dataDir = "data1k/";
	
	public static void main(String[] args) {
		System.out.println("Testing query 3 on 1k data." + System.getProperty("user.dir"));
		Socializer soc = new Socializer(dataDir, queries);
		soc.naiveApproach();
		//soc.optimizedApproach();
	}

}
