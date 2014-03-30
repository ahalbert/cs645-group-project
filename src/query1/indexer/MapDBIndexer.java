package query1.indexer;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.LineNumberReader;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Random;
import java.util.StringTokenizer;

import org.mapdb.BTreeKeySerializer;
import org.mapdb.BTreeMap;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Fun;
import org.mapdb.Pump;

/**
 * 
 * @author klimzaporojets
 *
 * Indexes using MapDB tool 
 */
public class MapDBIndexer {
	public void Index()
	{
		
	}
	
	public static void testDataPump(String[] args) throws IOException {
		// String
		// filePath="/Users/klimzaporojets/klim/umass/CMPSCI645 Database Design "
		// +
		// "and Implementation/project topics/social_networks/big_data_files/";
		// String fileName="comment_hasCreator_person_no_header.csv";

		String filePath = "/Users/klimzaporojets/klim/umass/CMPSCI645 Database Design "
				+ "and Implementation/project topics/social_networks/sorted_files/";
		String fileName = "comment_hasCreator_person_sf2.csv";

		FileReader fileReader = new FileReader(new File(filePath + fileName));

		LineNumberReader lnr = new LineNumberReader(fileReader);

		lnr.skip(Long.MAX_VALUE);
		final Integer max = lnr.getLineNumber();
		// Finally, the LineNumberReader object should be closed to prevent
		// resource leak
		lnr.close();
		final FileReader fileReader2 = new FileReader(new File(filePath
				+ fileName));
		final BufferedReader br = new BufferedReader(fileReader2);

		/** max number of elements to import */
		// final long max = (int) 1e7;

		/**
		 * Open database in temporary directory
		 */
		File dbFile = new File(
				"/Users/klimzaporojets/klim/umass/CMPSCI645 Database Design "
						+ "and Implementation/project topics/social_networks/sorted_files/mapdb.index");
		DB db = DBMaker.newFileDB(dbFile)
		/** disabling Write Ahead Log makes import much faster */
		.transactionDisable().make();

		// db.get(name)

		long time = System.currentTimeMillis();

		/**
		 * Source of data which randomly generates strings. In real world this
		 * would return data from file.
		 */
		Iterator<String> source = new Iterator<String>() {

			long counter = 0;

			@Override
			public boolean hasNext() {
				return counter < max;
			}

			@Override
			public String next() {
				counter++;
				String valueToReturn = "";
				try {
					String line = br.readLine();
					StringTokenizer st = new StringTokenizer(line, "|");
					st.nextToken();
					valueToReturn = st.nextToken();

				} catch (Exception ex) {
					ex.printStackTrace();
				}
				return valueToReturn;
				// return randomString(10);
			}

			@Override
			public void remove() {
			}
		};

		/**
		 * BTreeMap Data Pump requires data source to be presorted in reverse
		 * order (highest to lowest). There is method in Data Pump we can use to
		 * sort data. It uses temporarly files and can handle fairly large data
		 * sets.
		 */
		source = Pump.sort(source, true, 100000,
				Collections.reverseOrder(BTreeMap.COMPARABLE_COMPARATOR), // reverse
																			// order
																			// comparator
				db.getDefaultSerializer());

		// BTreeMap.COMPARABLE_COMPARATOR;
		/**
		 * Disk space used by serialized keys should be minimised. Keys are
		 * sorted, so only difference between consequential keys is stored. This
		 * method is called delta-packing and typically saves 60% of disk space.
		 */
		// BTreeKeySerializer<String> keySerializer = BTreeKeySerializer.TUPLE2
		// ;

		BTreeKeySerializer<String> keySerializer = BTreeKeySerializer.STRING;

		/**
		 * Translates Map Key into Map Value.
		 */
		Fun.Function1<Integer, String> valueExtractor = new Fun.Function1<Integer, String>() {
			@Override
			public Integer run(String s) {
				// return s.hashCode();
				return Integer.valueOf(s);
			}
		};
		// new Fun.Tuple2<>()

		/**
		 * Create BTreeMap and fill it with data
		 */
		Map<String, Integer> map = db.createTreeMap("map")
				.pumpSource(source, valueExtractor)
				// .pumpPresort(100000) // for presorting data we could also use
				// this method
				.keySerializer(keySerializer).make();

		System.out.println("Finished; total time: "
				+ (System.currentTimeMillis() - time) / 1000 + "s; there are "
				+ map.size() + " items in map");
		db.close();
	}
	public static void testMultiMap(String [] args) throws IOException
	{
		File dbFile = new File(
				"/Users/klimzaporojets/klim/umass/CMPSCI645 Database Design "
						+ "and Implementation/project topics/social_networks/sorted_files/multimapdb.index");
		
        DB db = //DBMaker.newMemoryDB().make();
        		DBMaker.newFileDB(dbFile).make();
        
		String filePath = "/Users/klimzaporojets/klim/umass/CMPSCI645 Database Design "
				+ "and Implementation/project topics/social_networks/sorted_files/";
		String fileName = "comment_hasCreator_person_sf2.csv";
        
        // this is wrong, do not do it !!!
        //  Map<String,List<Long>> map

        //correct way is to use composite set, where 'map key' is primary key and 'map value' is secondary value
        NavigableSet<Fun.Tuple2<Long,Long>> multiMap = db.getTreeSet("test");

        //optionally you can use set with Delta Encoding. This may save lot of space
        multiMap = db.createTreeSet("test2")
                .serializer(BTreeKeySerializer.TUPLE2)
                .make();

		final FileReader fileReader2 = new FileReader(new File(filePath
				+ fileName));
		final BufferedReader br = new BufferedReader(fileReader2);
        String line; 
        int counter =0; 
		while((line=br.readLine())!=null)
		{
			if(++counter%10000==0)
			{
				System.out.println(counter); 
			}
			StringTokenizer st = new StringTokenizer(line, "|");
			String commentId = st.nextToken();
			String personId = st.nextToken();
	        multiMap.add(Fun.t2(Long.valueOf(personId),Long.valueOf(personId)));
	        
		}
//        multiMap.add(Fun.t2("aa",1L));
//        multiMap.add(Fun.t2("aa",2L));
//        multiMap.add(Fun.t2("aa",3L));
//        multiMap.add(Fun.t2("bb",1L));

        //find all values for a key
//        for(Long l: Fun.filter(multiMap, "aa")){
//            System.out.println("value for key 'aa': "+l);
//        }
//
//        //check if pair exists
//
//        boolean found = multiMap.contains(Fun.t2("bb",1L));
//        System.out.println("Found: " + found);

        db.close();
	}
	
	//just a test 
	public static void main (String [] args) throws IOException
	{
//		testDataPump(args); 
		testMultiMap(args); 
		
	}
    public static String randomString(int size) {
        String chars = "0123456789abcdefghijklmnopqrstuvwxyz !@#$%^&*()_+=-{}[]:\",./<>?|\\";
        StringBuilder b = new StringBuilder(size);
        Random r = new Random();
        for(int i=0;i<size;i++){
            b.append(chars.charAt(r.nextInt(chars.length())));
        }
        return b.toString();
    }	
}
