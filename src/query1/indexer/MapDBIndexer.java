package query1.indexer;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.LineNumberReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Random;
import java.util.StringTokenizer;

import org.mapdb.BTreeKeySerializer;
import org.mapdb.BTreeMap;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Fun;
import org.mapdb.Fun.Tuple2;
import org.mapdb.Pump;
import org.mapdb.Serializer;

import com.google.common.base.Splitter;
import com.google.common.collect.*;
/**
 * 
 * @author klimzaporojets
 *
 * Indexes using MapDB tool 
 */
public class MapDBIndexer {
	
	static BTreeMap<Integer,String> treeMapCommentsForPerson = null; 
	static BTreeMap<Integer,Integer> treeMapCommentsResponseOfComments = null; 
	static BTreeMap<Integer,String> treeMapPersonKnowsPerson = null; 
	//step 1: index comment_to_person to access by comment and get the respective person_id
	//step 2: index iterate over person_to_person
	public static void Index() throws IOException
	{
		long time = System.currentTimeMillis(); 
		//step 1: sort the file person_knows_person by person1: already grouped, so this method doesn't sort so far 
		Iterator<Fun.Tuple2<Integer,Integer>> source = sortPersonKnowsPerson(); 
		
		//step 2: preload max of comment_response_of_comment from a particular person
		
		String fileOutputPath = "/Users/klimzaporojets/klim/umass/CMPSCI645 Database Design "
				+ "and Implementation/project topics/social_networks/sorted_files/final_index.csv";  

		File outputFile = new File(fileOutputPath);
		outputFile.delete(); 
		
		int counter = 0; 
		Integer currentPerson=null; 
		HashSet <String> commentsRepplied = new HashSet<String>(); 
		HashSet <String> commentsPersonB = new HashSet<String>(); 
		while(source.hasNext())
		{
			//++counter; 
			if(++counter%10000==0)
			{
				System.out.println("Read comments for: " + counter + " persons");
			}
			Fun.Tuple2<Integer, Integer> tuple2 = source.next();
			if(currentPerson==null)
			{
				commentsRepplied = getCommentsRepplied(Integer.valueOf(tuple2.a)); 
				currentPerson = tuple2.a; 
			}
			else
			{
				if(!currentPerson.equals(tuple2.a))
				{
					commentsRepplied = getCommentsRepplied(Integer.valueOf(tuple2.a)); 
					currentPerson = tuple2.a; 
					//System.out.println(counter); 
				}
			}
			commentsPersonB = getCommentsHash(tuple2.b);
			
			Integer setsIntersected = Sets.intersection(commentsRepplied, commentsPersonB).size();
			StringBuilder line = new StringBuilder("");
			line.append(tuple2.a);
			line.append("|");
			line.append(tuple2.b);
			line.append("|");
			line.append(setsIntersected);
			line.append("\n");
			writeTextFile(line.toString(), outputFile);
		}
		
		//step 3: 
		
		System.out.println("Final indexing time: "
				+ (System.currentTimeMillis() - time) / 1000 + " sec"); 
		buildIndexFinalFile(fileOutputPath);
	}
	
	public void getUsersConnected(Integer currentElement, Integer comments)
	{
		
	}
	
	static void buildIndexFinalFile(String pathFile) throws IOException
	{

		FileReader fileReader = new FileReader(new File(pathFile));

		LineNumberReader lnr = new LineNumberReader(fileReader);

		lnr.skip(Long.MAX_VALUE);
		final Integer max = lnr.getLineNumber();
		// Finally, the LineNumberReader object should be closed to prevent
		// resource leak
		lnr.close();
		final FileReader fileReader2 = new FileReader(new File(pathFile));
		final BufferedReader br = new BufferedReader(fileReader2);

		/**
		 * Open database in temporary directory
		 */
		File dbFile = new File(
				"/Users/klimzaporojets/klim/umass/CMPSCI645 Database Design "
						+ "and Implementation/project topics/social_networks/sorted_files/final_index.index");
		DB db = DBMaker.newFileDB(dbFile)
		/** disabling Write Ahead Log makes import much faster */
		.transactionDisable().make();

		long time = System.currentTimeMillis();

		/**
		 * Source of data which randomly generates strings. In real world this
		 * would return data from file.
		 */
		Iterator<Fun.Tuple2<String,Integer>> source = new Iterator<Fun.Tuple2<String,Integer>>(){
			long counter = 0;
			@Override
			public boolean hasNext() {
				// TODO Auto-generated method stub
				return counter < max;
			}

			@Override
			public Tuple2<String, Integer> next() {
				counter++;
				Integer personId1=0; 
				Integer personId2=0; 
				Integer quantity=0; 
				try {
					String line = br.readLine();
					StringTokenizer st = new StringTokenizer(line, "|");
					personId1 = Integer.valueOf(st.nextToken());
					personId2 = Integer.valueOf(st.nextToken());
					quantity = Integer.valueOf(st.nextToken());

				} catch (Exception ex) {
					ex.printStackTrace();
				}
				Fun.Tuple2<String, Integer> valueToReturn = new Fun.Tuple2<String, Integer>(personId1 + "_" + personId2, quantity);
				return valueToReturn;
				// return randomString(10);				
			}

			@Override
			public void remove() {
				// TODO Auto-generated method stub
				
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

		
		BTreeKeySerializer keySerializer = BTreeKeySerializer.STRING ; //BTreeKeySerializer.STRING;


		System.out.println("Sorting time: "
				+ (System.currentTimeMillis() - time) / 1000); 
		/**
		 * Create BTreeMap and fill it with data
		 * 
		 * TODO: for reverse order, override the compareTo method of Integer to behave the other way around
		 */
		Map<String, Integer> map = db.createTreeMap("map")
				.pumpSource(source).keySerializer(keySerializer).valueSerializer(Serializer.INTEGER)
				.make();

		System.out.println("Finished; total time to index comment_replyof_comment: "
				+ (System.currentTimeMillis() - time) / 1000 + "s; there are "
				+ map.size() + " items in map");
		db.close();			
	}
	static void writeTextFile(/*List<String> strLines*/ String line, File file) throws IOException {
		 Writer writer = new BufferedWriter(new OutputStreamWriter(
			        new FileOutputStream(file, true), "UTF-8"));
		 writer.append(line); 
		 writer.flush();
	}
	
//	public static void IndexWithCaching() throws IOException
//	{
//		//step 1: sort the file person_knows_person by person1: already grouped, so this method doesn't sort so far 
//		Iterator<Fun.Tuple2<Integer,Integer>> source = sortPersonKnowsPerson(); 
//		
//		//step 2: preload max of comment_response_of_comment from a particular person
//		
//		
//		int counter = 0; 
//		Integer currentPerson=null; 
//		HashSet <String> commentsRepplied = new HashSet<String>(); 
//		HashSet <String> commentsPersonB = new HashSet<String>(); 
//		while(source.hasNext())
//		{
//			++counter; 
////			if(++counter%100==0)
////			{
////				System.out.println("Read comments for: " + counter + " persons");
////			}
//			Fun.Tuple2<Integer, Integer> tuple2 = source.next();
//			if(currentPerson==null)
//			{
//				commentsRepplied = getCommentsRepplied(Integer.valueOf(tuple2.a)); 
//				currentPerson = tuple2.a; 
//			}
//			else
//			{
//				if(!currentPerson.equals(tuple2.a))
//				{
//					commentsRepplied = getCommentsRepplied(Integer.valueOf(tuple2.a)); 
//					currentPerson = tuple2.a; 
//					System.out.println(counter); 
//				}
//			}
//			//commentsPersonB = getCommentsHash(tuple2.b);
//			
//			Integer setsIntersected = Sets.intersection(commentsRepplied, commentsPersonB).size();
//			
//			//
//			
//		}
//		
//		//step 3: 
//		
//		
//	}	
	public static HashSet<String> getCommentsHash(Integer personId)
	{
		String comments = getCommentsForPerson(personId);
		if(comments == null)
		{
			return new HashSet<String>(); 
		}
		return Sets.newHashSet((Splitter.on(",").split(comments)));
	}
	public static HashSet<String> getCommentsRepplied(Integer personId)
	{
		String comments = getCommentsForPerson(personId);
		HashSet<String> commentsRepplied = new HashSet<String>(); 
		if(comments==null)
		{
			return commentsRepplied;
		}
		StringTokenizer st = new StringTokenizer(comments, ",");
		while(st.hasMoreTokens())
		{
			commentsRepplied.add(String.valueOf(getCommentReplied(Integer.valueOf(st.nextToken()))));
		}
		return commentsRepplied; 
	}
	public static Iterator<Fun.Tuple2<Integer,Integer>> sortPersonKnowsPerson() throws IOException
	{
		String filePath = "/Users/klimzaporojets/klim/umass/CMPSCI645 Database Design "
				+ "and Implementation/project topics/social_networks/big_data_files/";
		String fileName = "person_knows_person.csv"; //"comment_replyOf_comment_no_header.csv";
		
		final FileReader fileReader2 = new FileReader(new File(filePath
				+ fileName));
		FileReader fileReader = new FileReader(new File(filePath + fileName));

		final BufferedReader br = new BufferedReader(fileReader2);
		LineNumberReader lnr = new LineNumberReader(fileReader);

		lnr.skip(Long.MAX_VALUE);
		final Integer max = lnr.getLineNumber();
		// Finally, the LineNumberReader object should be closed to prevent
		// resource leak
		lnr.close();
		Iterator<Fun.Tuple2<Integer,Integer>> source = new Iterator<Fun.Tuple2<Integer,Integer>>(){
			long counter = 0;
			@Override
			public boolean hasNext() {
				// TODO Auto-generated method stub
				return counter < max-1;
			}

			@Override
			public Tuple2<Integer, Integer> next() {
				counter++;
				//if the first line, skip because of the header
				if(counter==1)
				{
					try
					{
						br.readLine();
					}
					catch(Exception ex)
					{
						ex.printStackTrace();
					}
				}
				Integer commentId1=0; 
				Integer commentId2=0; 
				try {
					String line = br.readLine();
					StringTokenizer st = new StringTokenizer(line, "|");
					commentId1 = Integer.valueOf(st.nextToken());
					commentId2 = Integer.valueOf(st.nextToken());

				} catch (Exception ex) {
					ex.printStackTrace();
				}
				Fun.Tuple2<Integer, Integer> valueToReturn = new Fun.Tuple2<Integer, Integer>(commentId1, commentId2);
				return valueToReturn;
				// return randomString(10);				
			}

			@Override
			public void remove() {
				// TODO Auto-generated method stub
				
			}		
		};
		return source; 
	}
	
	//gets the comments for a particular person 
	public static String getCommentsForPerson(Integer personId)
	{
		if(treeMapCommentsForPerson==null)
		{
			File dbFile = new File(
				"/Users/klimzaporojets/klim/umass/CMPSCI645 Database Design "
						+ "and Implementation/project topics/social_networks/sorted_files/mapdb.index");
			DB db = DBMaker.newFileDB(dbFile)
					/** disabling Write Ahead Log makes import much faster */
					.transactionDisable().make();
		
			treeMapCommentsForPerson = db.getTreeMap("map");
		}
		String valOftreeMap = treeMapCommentsForPerson.get(personId);
		return valOftreeMap; 
	}
	
	public String getPersonKnowsPerson(Integer personId)
	{
		if(treeMapPersonKnowsPerson==null)
		{
			File dbFile = new File(
				"/Users/klimzaporojets/klim/umass/CMPSCI645 Database Design "
						+ "and Implementation/project topics/social_networks/sorted_files/mapdb_person_knows_person.index");
			DB db = DBMaker.newFileDB(dbFile)
					/** disabling Write Ahead Log makes import much faster */
					.transactionDisable().make();
		
			treeMapCommentsForPerson = db.getTreeMap("map");
		}
		String valOftreeMap = treeMapCommentsForPerson.get(personId);
		return valOftreeMap; 
		
	}
	//gets the comment replied by a particular comment
	public static Integer getCommentReplied(Integer commentId)
	{
		if(treeMapCommentsResponseOfComments==null)
		{
			File dbFile = new File(
					"/Users/klimzaporojets/klim/umass/CMPSCI645 Database Design "
							+ "and Implementation/project topics/social_networks/sorted_files/mapdb_comm_rof_comm.index");
			DB db = DBMaker.newFileDB(dbFile)
			/** disabling Write Ahead Log makes import much faster */
			.transactionDisable().readOnly().make();
			treeMapCommentsResponseOfComments = db.getTreeMap("map");
		}
		
		Integer valOftreeMap = treeMapCommentsResponseOfComments.get(commentId);
		return valOftreeMap; 
	}
	public ArrayList<Integer> getUsersConnected()
	{
		String persons = getPersonKnowsPerson(personId);
		HashSet<String> commentsRepplied = new HashSet<String>(); 
		if(persons==null)
		{
			return commentsRepplied;
		}
		StringTokenizer st = new StringTokenizer(persons, ",");
		while(st.hasMoreTokens())
		{
			commentsRepplied.add(String.valueOf(getCommentReplied(Integer.valueOf(st.nextToken()))));
		}
		return commentsRepplied; 
		
	}
	public static void indexPersonKnowsPerson(String[] args) throws IOException {
		String filePath = "/Users/klimzaporojets/klim/umass/CMPSCI645 Database Design "
				+ "and Implementation/project topics/social_networks/big_data_files/";
		String fileName = "person_knows_person.csv"; //"comment_replyOf_comment_no_header.csv";

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

		
		
		/**
		 * Open database in temporary directory
		 */
		File dbFile = new File(
				"/Users/klimzaporojets/klim/umass/CMPSCI645 Database Design "
						+ "and Implementation/project topics/social_networks/sorted_files/mapdb_person_knows_person.index");
		DB db = DBMaker.newFileDB(dbFile)
		/** disabling Write Ahead Log makes import much faster */
		.transactionDisable().make();

		// db.get(name)

		long time = System.currentTimeMillis();

		/**
		 * Source of data which randomly generates strings. In real world this
		 * would return data from file.
		 */
		Iterator<Fun.Tuple2<Integer,String>> source = new Iterator<Fun.Tuple2<Integer,String>>(){
			long counter = 0;
			@Override
			public boolean hasNext() {
				// TODO Auto-generated method stub
				return counter < max-1;
			}

			@Override
			public Tuple2<Integer, String> next() {
				counter++;
				Integer personId1=0; 
				String personId2=""; 
				try {
					//ignores the header 
					if(counter==1)
					{
						br.readLine(); 
					}
					String line = br.readLine();
					StringTokenizer st = new StringTokenizer(line, "|");
					personId1 = Integer.valueOf(st.nextToken());
					personId2 = st.nextToken();

				} catch (Exception ex) {
					ex.printStackTrace();
				}
				Fun.Tuple2<Integer, String> valueToReturn = new Fun.Tuple2<Integer, String>(personId1, personId2);
				return valueToReturn;
				// return randomString(10);				
			}

			@Override
			public void remove() {
				// TODO Auto-generated method stub
				
			}
			
		};

		source = Pump.sort(source, true, 100000,
				Collections.reverseOrder(BTreeMap.COMPARABLE_COMPARATOR), // reverse
																			// order
																			// comparator
				db.getDefaultSerializer());

		int counter = 0; 

		System.out.println ("counter: " + counter);
		
		
		BTreeKeySerializer keySerializer = BTreeKeySerializer.BASIC ; //BTreeKeySerializer.STRING;



		System.out.println("Sorting time: "
				+ (System.currentTimeMillis() - time) / 1000); 
		/**
		 * Create BTreeMap and fill it with data
		 * 
		 * TODO: for reverse order, override the compareTo method of Integer to behave the other way around
		 */
//		Map<String, Integer> map = db.createTreeMap("map")
//				.pumpSource(source).keySerializer(keySerializer).valueSerializer(Serializer.INTEGER)
//				.make();
		
		Map<String, Integer> map = db.createTreeMap("map")
				.pumpSource(source).keySerializer(keySerializer).valueSerializer(Serializer.STRING)
				.pumpIgnoreDuplicates()
				.makeAdapt2();
		
		System.out.println("Finished; total time to index person_knows_person: "
				+ (System.currentTimeMillis() - time) / 1000 + "s; there are "
				+ map.size() + " items in map");
		db.close();	
	}	
	
	public static void indexCommentReplyOfComment(String[] args) throws IOException {
		String filePath = "/Users/klimzaporojets/klim/umass/CMPSCI645 Database Design "
				+ "and Implementation/project topics/social_networks/big_data_files/";
		String fileName = "comment_replyOf_comment_reverse.csv"; //"comment_replyOf_comment_no_header.csv";

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

		/**
		 * Open database in temporary directory
		 */
		File dbFile = new File(
				"/Users/klimzaporojets/klim/umass/CMPSCI645 Database Design "
						+ "and Implementation/project topics/social_networks/sorted_files/mapdb_comm_rof_comm.index");
		DB db = DBMaker.newFileDB(dbFile)
		/** disabling Write Ahead Log makes import much faster */
		.transactionDisable().make();

		// db.get(name)

		long time = System.currentTimeMillis();

		/**
		 * Source of data which randomly generates strings. In real world this
		 * would return data from file.
		 */
		Iterator<Fun.Tuple2<Integer,Integer>> source = new Iterator<Fun.Tuple2<Integer,Integer>>(){
			long counter = 0;
			@Override
			public boolean hasNext() {
				// TODO Auto-generated method stub
				return counter < max;
			}

			@Override
			public Tuple2<Integer, Integer> next() {
				counter++;
				Integer commentId1=0; 
				Integer commentId2=0; 
				try {
					String line = br.readLine();
					StringTokenizer st = new StringTokenizer(line, "|");
					commentId1 = Integer.valueOf(st.nextToken());
					commentId2 = Integer.valueOf(st.nextToken());

				} catch (Exception ex) {
					ex.printStackTrace();
				}
				Fun.Tuple2<Integer, Integer> valueToReturn = new Fun.Tuple2<Integer, Integer>(commentId1, commentId2);
				return valueToReturn;
				// return randomString(10);				
			}

			@Override
			public void remove() {
				// TODO Auto-generated method stub
				
			}
			
		};


		int counter = 0; 

		System.out.println ("counter: " + counter);
		
		
		BTreeKeySerializer keySerializer = BTreeKeySerializer.BASIC ; //BTreeKeySerializer.STRING;



		System.out.println("Sorting time: "
				+ (System.currentTimeMillis() - time) / 1000); 
		/**
		 * Create BTreeMap and fill it with data
		 * 
		 * TODO: for reverse order, override the compareTo method of Integer to behave the other way around
		 */
		Map<String, Integer> map = db.createTreeMap("map")
				.pumpSource(source).keySerializer(keySerializer).valueSerializer(Serializer.INTEGER)
				.make();

		System.out.println("Finished; total time to index comment_replyof_comment: "
				+ (System.currentTimeMillis() - time) / 1000 + "s; there are "
				+ map.size() + " items in map");
		db.close();	
	}
	public static void indexCommentHasCreatorPerson(String[] args) throws IOException {


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
		Iterator<Fun.Tuple2<Integer,String>> source = new Iterator<Fun.Tuple2<Integer,String>>(){
			long counter = 0;
			@Override
			public boolean hasNext() {
				// TODO Auto-generated method stub
				return counter < max;
			}

			@Override
			public Tuple2<Integer, String> next() {
				counter++;
				String commentId=""; 
				Integer personId=0; 
				try {
					String line = br.readLine();
					StringTokenizer st = new StringTokenizer(line, "|");
					commentId = st.nextToken().toString();
					personId = Integer.valueOf(st.nextToken());

				} catch (Exception ex) {
					ex.printStackTrace();
				}
				Fun.Tuple2<Integer, String> valueToReturn = new Fun.Tuple2<Integer, String>(personId, commentId);
				return valueToReturn;
				// return randomString(10);				
			}

			@Override
			public void remove() {
				// TODO Auto-generated method stub
				
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


		int counter = 0; 

		System.out.println ("counter: " + counter);
		
		
		BTreeKeySerializer keySerializer = BTreeKeySerializer.BASIC ; //BTreeKeySerializer.STRING;


		System.out.println("Sorting time: "
				+ (System.currentTimeMillis() - time) / 1000); 
		/**
		 * Create BTreeMap and fill it with data
		 */
		Map<String, Integer> map = db.createTreeMap("map")
				.pumpSource(source).keySerializer(keySerializer).valueSerializer(Serializer.STRING)
				.pumpIgnoreDuplicates()
				.makeAdapt2();

		System.out.println("Finished; total time: "
				+ (System.currentTimeMillis() - time) / 1000 + "s; there are "
				+ map.size() + " items in map");
		db.close();
	}
	
	
//	//TODO: compare the insertions in documentation 
//	public static void testMultiMap(String [] args) throws IOException
//	{
//		File dbFile = new File(
//				"/Users/klimzaporojets/klim/umass/CMPSCI645 Database Design "
//						+ "and Implementation/project topics/social_networks/sorted_files/multimapdb.index");
//		
//        DB db = //DBMaker.newMemoryDB().make();
//        		DBMaker.newFileDB(dbFile).make();
////        DBMaker.
//        
//        
//		String filePath = "/Users/klimzaporojets/klim/umass/CMPSCI645 Database Design "
//				+ "and Implementation/project topics/social_networks/sorted_files/";
//		String fileName = "comment_hasCreator_person_sf2.csv";
//        
//        // this is wrong, do not do it !!!
//        //  Map<String,List<Long>> map
//
//        //correct way is to use composite set, where 'map key' is primary key and 'map value' is secondary value
//        NavigableSet<Fun.Tuple2<Long,Long>> multiMap = db.getTreeSet("test2");
//
//        //optionally you can use set with Delta Encoding. This may save lot of space
////        multiMap = db.createTreeSet("test2")
////                .serializer(BTreeKeySerializer.TUPLE2)
////                .make();
//
//		final FileReader fileReader2 = new FileReader(new File(filePath
//				+ fileName));
//		final BufferedReader br = new BufferedReader(fileReader2);
//        String line; 
//        int counter =0; 
//		while((line=br.readLine())!=null)
//		{
//			if(++counter%100000==0)
//			{
//				System.out.println(counter); 
//			}
//			StringTokenizer st = new StringTokenizer(line, "|");
//			String commentId = st.nextToken();
//			String personId = st.nextToken();
//	        multiMap.add(Fun.t2(Long.valueOf(personId),Long.valueOf(commentId)));
//	        
//		}
//
//
//        //find all values for a key
////        for(Long l: Fun.filter(multiMap, 9999L)){
////            System.out.println("value for key 'person': "+l);
////        }
////
////        //check if pair exists
////
////        boolean found = multiMap.contains(Fun.t2("bb",1L));
////        System.out.println("Found: " + found);
//
//        db.close();
//	}
	
	//just a test 
	public static void main (String [] args) throws IOException
	{
//		retrieveDataPump(args); 
//		testDataPump(args); 
//		testMultiMap(args);
//		indexCommentReplyOfComment(args);
		indexPersonKnowsPerson(args); 
		//Index();
//		buildIndexFinalFile("/Users/klimzaporojets/klim/umass/CMPSCI645 Database Design "
//				+ "and Implementation/project topics/social_networks/sorted_files/final_index.csv");
		
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
