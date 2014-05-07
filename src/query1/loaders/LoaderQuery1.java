package query1.loaders;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.SortedMap;
import java.util.StringTokenizer;
import java.util.concurrent.ConcurrentHashMap;

import com.jramoyo.io.IndexedFileReader;

import query1.dtos.Comment;

/**
 * 
 * @author klimzaporojets
 * Loads data for query1 
 */
class InsertionThread implements Runnable {
	int from; 
	int to; 
	String path; 
	int id; 
	boolean flag = false; 
	HashMap<Integer, Integer> commentToPersonS = null; 
	   public InsertionThread(int from, int to, String path, int id, HashMap<Integer, Integer> commentToPersonS ) {
	       // store parameter for later user
		   this.from = from; 
		   this.to = to; 
		   this.path = path; 
		   this.id = id; 
		   this.commentToPersonS = commentToPersonS;
	   }

	   public void run() {
		   try
		   {
			   BufferedReader br = new BufferedReader(new FileReader(path));
			   String line; 
			   int counter=0; 
			   
			   while ((line = br.readLine()) != null & counter<to)  
			   {
				   
				   if(++counter>=from)
				   {
					   if(!flag)
					   {
						   flag=true; 
						   System.out.println(id + " " + line);
					   }
					   if(counter==1)
					   {
						   counter++;
						   line = br.readLine();
					   }
//					   if(counter%10000==0)
//					   {
//						   System.out.println(counter);
//					   }
						StringTokenizer st = new StringTokenizer(line,"|");
						Integer commentId = Integer.valueOf(st.nextToken());
						Integer creatorId = Integer.valueOf(st.nextToken());
//						if(commentId%4==0)
						commentToPersonS.put(commentId, creatorId);
//						synchronized(LoaderQuery1.commentToPersonS)
//						{
//						}
				   }
			   }
			   System.out.println("ready " + counter);
			   LoaderQuery1.counter--; 
		   }catch(Exception ex)
		   {
			   ex.printStackTrace();
		   }
	   }
	}

public class LoaderQuery1 {
	
	
	HashMap<Comment, Integer> commentsConnectedPersons = new HashMap<Comment, Integer>();
	public static int counter = 4;
//	public static ConcurrentHashMap<Integer, Integer> commentToPersonS = new ConcurrentHashMap<Integer, Integer>();
	public static HashMap<Integer,Integer> commentToPersonS1[] = new HashMap[4];
	static{
	commentToPersonS1[0] = new HashMap<Integer,Integer>();
	commentToPersonS1[1] = new HashMap<Integer,Integer>();
	commentToPersonS1[2] = new HashMap<Integer,Integer>();
	commentToPersonS1[3] = new HashMap<Integer,Integer>();
	}
//	public static HashMap<Integer, Integer> commentToPersonS5 = new HashMap<Integer,Integer>();
//	public static HashMap<Integer, Integer> commentToPersonS6 = new HashMap<Integer,Integer>();
//	public static HashMap<Integer, Integer> commentToPersonS7 = new HashMap<Integer,Integer>();
//	public static HashMap<Integer, Integer> commentToPersonS8 = new HashMap<Integer,Integer>();
//	public static HashMap<Integer, Integer> commentToPersonS9 = new HashMap<Integer,Integer>();
//	public static HashMap<Integer, Integer> commentToPersonS10 = new HashMap<Integer,Integer>();
//	public static HashMap<Integer, Integer> commentToPersonS11 = new HashMap<Integer,Integer>();
//	public static HashMap<Integer, Integer> commentToPersonS12 = new HashMap<Integer,Integer>();
//	public static HashMap<Integer, Integer> commentToPersonS13 = new HashMap<Integer,Integer>();
//	public static HashMap<Integer, Integer> commentToPersonS14 = new HashMap<Integer,Integer>();
//	public static HashMap<Integer, Integer> commentToPersonS15 = new HashMap<Integer,Integer>();
//	public static HashMap<Integer, Integer> commentToPersonS16 = new HashMap<Integer,Integer>();
//	public static HashMap<Integer, Integer> commentToPersonS17 = new HashMap<Integer,Integer>();
//	public static HashMap<Integer, Integer> commentToPersonS18 = new HashMap<Integer,Integer>();
//	public static HashMap<Integer, Integer> commentToPersonS19 = new HashMap<Integer,Integer>();
//	public static HashMap<Integer, Integer> commentToPersonS20 = new HashMap<Integer,Integer>();
	
//	static{
//		commentToPersonS[0] = new HashMap<Integer,Integer>(); 
//		commentToPersonS[1] = new HashMap<Integer,Integer>(); 
//		commentToPersonS[2] = new HashMap<Integer,Integer>(); 
//		commentToPersonS[3] = new HashMap<Integer,Integer>(); 
//	}
	public HashMap<Integer,ArrayList<Integer>> loadDataThreads(Integer from, Integer to, Integer numberOfComments, String dataPath) throws Exception
	{
//		BufferedReader br = new BufferedReader(new FileReader(dataPath + "/person_knows_person.csv"));  
//		String line = null;  
//		br.readLine(); 
//		while ((line = br.readLine()) != null)  
//		{	
		
		long time = System.currentTimeMillis(); 
		File f = new File(dataPath + "/comment_replyOf_comment.csv");
		
		IndexedFileReader reader = new IndexedFileReader(f);
		SortedMap<Integer, String> res = reader.readLines(4000000, 5000000);
		
		System.out.println("Time to read a range of 1,000,000 lines: " + (System.currentTimeMillis() - time));
		return null; 
//		long number_of_lines_read = 0; 
//		HashMap<Integer, ArrayList<Integer>> query1Data = new HashMap<Integer, ArrayList<Integer>>();
//		try
//		{
//			BufferedReader br = new BufferedReader(new FileReader(dataPath + "/person_knows_person.csv"));  
//			String line = null;  
//			br.readLine(); 
//			while ((line = br.readLine()) != null)  
//			{
////			number_of_lines_read++; 
////				if(number_of_lines_read%100==0)
////				{
////					System.out.println(number_of_lines_read);
////				}
//				
//				//StringTokenizer may be much faster: 
//				//http://www.javamex.com/tutorials/regular_expressions/splitting_tokenisation_performance.shtml#.UxoFNV6YS18
//				StringTokenizer st = new StringTokenizer(line,"|");
//				Integer knowsFrom = Integer.valueOf(st.nextToken());
//				Integer knowsTo = Integer.valueOf(st.nextToken());
//				//System.out.println(knowsFrom + " : " + knowsTo);
//				
//				/*begin: check the number of comments*/
//				boolean doBelong = false; 
//				if(numberOfComments==-1)
//				{
//					doBelong=true; 
//				}
//				else
//				{
//					doBelong = isNumberOfCommentsGreaterThanV2(knowsFrom,knowsTo,numberOfComments, dataPath);
//				}
//				/*end: check the number of comments*/
//				if(doBelong==true)
//				{
//					ArrayList<Integer> whomKnows = query1Data.get(knowsFrom);
//					if(whomKnows==null)
//					{
//						whomKnows = new ArrayList<Integer>();
//					}
//					whomKnows.add(knowsTo);
//					query1Data.put(knowsFrom, whomKnows);
//				}				
//			} 
//			br.close();
//		}
//		catch(Exception ex)
//		{
//			 ex.printStackTrace(); 
//		}
//		return query1Data; 
	}	
	public HashMap<Integer,ArrayList<Integer>> loadData(Integer from, Integer to, Integer numberOfComments, String dataPath)
	{
		long number_of_lines_read = 0; 
		HashMap<Integer, ArrayList<Integer>> query1Data = new HashMap<Integer, ArrayList<Integer>>();
		try
		{
			BufferedReader br = new BufferedReader(new FileReader(dataPath + "/person_knows_person.csv"));  
			String line = null;  
			br.readLine(); 
			while ((line = br.readLine()) != null)  
			{
				number_of_lines_read++; 
//				if(number_of_lines_read%100==0)
//				{
//					System.out.println(number_of_lines_read);
//				}
				
				//StringTokenizer may be much faster: 
				//http://www.javamex.com/tutorials/regular_expressions/splitting_tokenisation_performance.shtml#.UxoFNV6YS18
				StringTokenizer st = new StringTokenizer(line,"|");
				Integer knowsFrom = Integer.valueOf(st.nextToken());
				Integer knowsTo = Integer.valueOf(st.nextToken());
				//System.out.println(knowsFrom + " : " + knowsTo);
				
				/*begin: check the number of comments*/
				boolean doBelong = false; 
				if(numberOfComments==-1)
				{
					doBelong=true; 
				}
				else
				{
					doBelong = isNumberOfCommentsGreaterThanV2(knowsFrom,knowsTo,numberOfComments, dataPath);
				}
				/*end: check the number of comments*/
				if(doBelong==true)
				{
					ArrayList<Integer> whomKnows = query1Data.get(knowsFrom);
					if(whomKnows==null)
					{
						whomKnows = new ArrayList<Integer>();
					}
					whomKnows.add(knowsTo);
					query1Data.put(knowsFrom, whomKnows);
				}				
			} 
			br.close();
		}
		catch(Exception ex)
		{
			 ex.printStackTrace(); 
		}
		return query1Data; 
	}
	
	public boolean isNumberOfCommentsGreaterThanV1(Integer knowsFrom, Integer knowsTo, Integer numberOfComments, String dataPath)
	{
		//step 1: gets the comments of knowsTo from comment_hasCreator_person
		//step 2: starts counting the answers of knowsFrom checking if these answers are for comments read in step 1. 
		//this is done reading the file comment_replyOf_comment
		long lines_read = 0; 
		BufferedReader br = null; 
		try{
			//step 1 start
			br = new BufferedReader(new FileReader(dataPath + "/comment_hasCreator_person.csv"));  
			String line = null;  
			br.readLine();
			
			//HashMap<Integer, ArrayList<Integer>> commentsPerPerson = new HashMap<Integer, ArrayList<Integer>>();
			HashSet<Integer> commentsKnowsTo = new HashSet<Integer>(); 
			HashSet<Integer> commentsKnowsFrom = new HashSet<Integer>(); 
			
			while ((line = br.readLine()) != null)  
			{
				lines_read++; 
				if(lines_read%1000==0)
				{
					//System.out.println("has creator lines read: " + lines_read);
				}
				StringTokenizer st = new StringTokenizer(line,"|");
				Integer commentId = Integer.valueOf(st.nextToken());
				Integer personId = Integer.valueOf(st.nextToken());
				if(personId.equals(knowsTo))
				{
					commentsKnowsTo.add(commentId);
				}
				if(personId.equals(knowsFrom))
				{
					commentsKnowsFrom.add(commentId);
				}
			}
			
			
			
			//step 1 end 
			//step 2 start 
			br.close();
			br = new BufferedReader(new FileReader(dataPath + "/comment_replyOf_comment.csv"));  
			line = null;  
			br.readLine();
			
			//counter that counts the number of comments replied 
			
			int counter = 0; 
						
			while ((line = br.readLine()) != null)  
			{
				StringTokenizer st = new StringTokenizer(line,"|");
				Integer replyId = Integer.valueOf(st.nextToken());
				Integer commentId = Integer.valueOf(st.nextToken());
				if(commentsKnowsTo.contains(commentId)&&commentsKnowsFrom.contains(replyId))
				{
					counter += 1; 
				}
				if(counter>numberOfComments)
				{
					br.close();
					return true; 
				}
			}
			
			
		}
		catch(Exception ex)
		{
			//do something about it? 
		}
		finally{
			try
			{
				if(br!=null)
				{
					br.close();
				}
			}
			catch(Exception ex)
			{
				
			}
		}
		return false; 
	}
	
	public boolean isNumberOfCommentsGreaterThanV2(Integer knowsFrom, Integer knowsTo, Integer numberOfComments, String dataPath)
	{
		//step 0: pre-loads all the data (only if not preloaded before)
		//step 1: gets the comments of knowsTo from comment_hasCreator_person
		//step 2: starts counting the answers of knowsFrom checking if these answers are for comments read in step 1. 
		//this is done reading the file comment_replyOf_comment
		if(commentsConnectedPersons.size()==0)
		{
			//doPreload(dataPath); 
			doPreloadImproved(dataPath, numberOfComments); 
		}
		Comment comment = new Comment(); 
		comment.setUserIdFrom(knowsFrom);
		comment.setUserIdTo(knowsTo);
		
		Integer number = commentsConnectedPersons.get(comment);
		if(number!=null)
		{
			if(number>numberOfComments)
			{
				//the other way around
				comment.setUserIdFrom(knowsTo);
				comment.setUserIdTo(knowsFrom);
				
				number = commentsConnectedPersons.get(comment);
				if(number!=null && number>numberOfComments)
				{
					return true;
				}
			}
		}
		return false; 
	}	
	
	public void doPreload(String dataPath)
	{
		BufferedReader br = null; 
		try{
			//step 1 start
			br = new BufferedReader(new FileReader(dataPath + "/comment_hasCreator_person.csv"));  
			String line = null;  
			br.readLine();
			
			//HashMap<Integer, ArrayList<Integer>> commentsPerPerson = new HashMap<Integer, ArrayList<Integer>>();
//			HashSet<Integer> commentsKnowsTo = new HashSet<Integer>(); 
//			HashSet<Integer> commentsKnowsFrom = new HashSet<Integer>(); 
			
			HashMap<Integer, Integer> commentToPerson = new HashMap<Integer, Integer>(); 
			
			long time1 = System.currentTimeMillis(); 
			while ((line = br.readLine()) != null)  
			{
				StringTokenizer st = new StringTokenizer(line,"|");
				Integer commentId = Integer.valueOf(st.nextToken());
				Integer creatorId = Integer.valueOf(st.nextToken());
//				String commentId = st.nextToken();
//				String creatorId = st.nextToken(); 
				commentToPerson.put(commentId, creatorId);
			}
			System.out.println("Time to load comment_hasCreator_person: " + (System.currentTimeMillis()-time1));
			
			
			//step 1 end 
			//step 2 start 
			br.close();
			br = new BufferedReader(new FileReader(dataPath + "/comment_replyOf_comment.csv"));  
			line = null;  
			br.readLine();
			
			//counter that counts the number of comments replied 
			
			int counter = 0; 
						
			while ((line = br.readLine()) != null)  
			{
				StringTokenizer st = new StringTokenizer(line,"|");
				Integer replyId = Integer.valueOf(st.nextToken());
				Integer commentId = Integer.valueOf(st.nextToken());
				
				Integer commentFrom = commentToPerson.get(replyId); 
				Integer commentTo = commentToPerson.get(commentId); 
				Comment comment = new Comment(); 
				comment.setUserIdFrom(commentFrom);
				comment.setUserIdTo(commentTo);
				
				
				Integer number = commentsConnectedPersons.get(comment);
				if(number==null)
				{
					number=1; 
				}
				else
				{
					number++; 
				}
				commentsConnectedPersons.put(comment, number);
			}
			
			
		}
		catch(Exception ex)
		{
			//do something about it? 
		}
		finally{
			try
			{
				if(br!=null)
				{
					br.close();
				}
			}
			catch(Exception ex)
			{
				
			}
		}
	}
	public void doPreloadImproved(String dataPath, int comments)
	{
		long time2 = System.currentTimeMillis(); 
		Runnable r = new InsertionThread(1,5000000, dataPath + "/comment_hasCreator_person.csv",0,commentToPersonS1[0]);
		Thread t = new Thread(r); 
		t.setPriority(Thread.MAX_PRIORITY);
		t.start();
		Runnable r2 = new InsertionThread(5000000,10000000, dataPath + "/comment_hasCreator_person.csv",1,commentToPersonS1[1]);
		Thread t2 = new Thread(r2); 
		t2.setPriority(Thread.MAX_PRIORITY);
		t2.start();
		Runnable r3 = new InsertionThread(10000000,15000000, dataPath + "/comment_hasCreator_person.csv",2,commentToPersonS1[2]);
		Thread t3 = new Thread(r3); 
		t3.setPriority(Thread.MAX_PRIORITY);
		t3.start();
		Runnable r4 = new InsertionThread(15000000,99999999, dataPath + "/comment_hasCreator_person.csv",3,commentToPersonS1[3]);
		Thread t4 = new Thread(r4); 
		t4.setPriority(Thread.MAX_PRIORITY);
		t4.start();
//		Runnable r5 = new InsertionThread(12000000,15000000, dataPath + "/comment_hasCreator_person.csv",3,commentToPersonS5);
//		new Thread(r5).start(); 
//		Runnable r6 = new InsertionThread(15000000,18000000, dataPath + "/comment_hasCreator_person.csv",3,commentToPersonS6);
//		new Thread(r6).start(); 
//		Runnable r7 = new InsertionThread(18000000,99999999, dataPath + "/comment_hasCreator_person.csv",3,commentToPersonS7);
//		new Thread(r7).start(); 
//		Runnable r8 = new InsertionThread(14000000,16000000, dataPath + "/comment_hasCreator_person.csv",3,commentToPersonS8);
//		new Thread(r8).start(); 
//		Runnable r9 = new InsertionThread(16000000,18000000, dataPath + "/comment_hasCreator_person.csv",3,commentToPersonS9);
//		new Thread(r9).start(); 
//		Runnable r10 = new InsertionThread(18000000,99999999, dataPath + "/comment_hasCreator_person.csv",3,commentToPersonS10);
//		new Thread(r10).start(); 

//		Runnable r = new InsertionThread(1,1000000, dataPath + "/comment_hasCreator_person.csv",0,commentToPersonS1);
//		new Thread(r).start(); 
//		Runnable r2 = new InsertionThread(1000000,2000000, dataPath + "/comment_hasCreator_person.csv",1,commentToPersonS2);
//		new Thread(r2).start(); 
//		Runnable r3 = new InsertionThread(2000000,3000000, dataPath + "/comment_hasCreator_person.csv",2,commentToPersonS3);
//		new Thread(r3).start(); 
//		Runnable r4 = new InsertionThread(3000000,4000000, dataPath + "/comment_hasCreator_person.csv",3,commentToPersonS4);
//		new Thread(r4).start(); 
//		Runnable r5 = new InsertionThread(4000000,5000000, dataPath + "/comment_hasCreator_person.csv",0,commentToPersonS5);
//		new Thread(r5).start(); 
//		Runnable r6 = new InsertionThread(5000000,6000000, dataPath + "/comment_hasCreator_person.csv",1,commentToPersonS6);
//		new Thread(r6).start(); 
//		Runnable r7 = new InsertionThread(6000000,7000000, dataPath + "/comment_hasCreator_person.csv",2,commentToPersonS7);
//		new Thread(r7).start(); 
//		Runnable r8 = new InsertionThread(7000000,8000000, dataPath + "/comment_hasCreator_person.csv",3,commentToPersonS8);
//		new Thread(r8).start(); 
//		Runnable r9 = new InsertionThread(8000000,9000000, dataPath + "/comment_hasCreator_person.csv",0,commentToPersonS9);
//		new Thread(r9).start(); 
//		Runnable r10 = new InsertionThread(9000000,10000000, dataPath + "/comment_hasCreator_person.csv",1,commentToPersonS10);
//		new Thread(r10).start(); 
//		Runnable r11 = new InsertionThread(10000000,11000000, dataPath + "/comment_hasCreator_person.csv",2,commentToPersonS11);
//		new Thread(r11).start(); 
//		Runnable r12 = new InsertionThread(11000000,12000000, dataPath + "/comment_hasCreator_person.csv",3,commentToPersonS12);
//		new Thread(r12).start(); 
//		Runnable r13 = new InsertionThread(12000000,13000000, dataPath + "/comment_hasCreator_person.csv",0,commentToPersonS13);
//		new Thread(r13).start(); 
//		Runnable r14 = new InsertionThread(13000000,14000000, dataPath + "/comment_hasCreator_person.csv",1,commentToPersonS14);
//		new Thread(r14).start(); 
//		Runnable r15 = new InsertionThread(14000000,15000000, dataPath + "/comment_hasCreator_person.csv",2,commentToPersonS15);
//		new Thread(r15).start(); 
//		Runnable r16 = new InsertionThread(15000000,16000000, dataPath + "/comment_hasCreator_person.csv",3,commentToPersonS16);
//		new Thread(r16).start(); 
//		Runnable r17 = new InsertionThread(16000000,17000000, dataPath + "/comment_hasCreator_person.csv",0,commentToPersonS17);
//		new Thread(r17).start(); 
//		Runnable r18 = new InsertionThread(17000000,18000000, dataPath + "/comment_hasCreator_person.csv",1,commentToPersonS18);
//		new Thread(r18).start(); 
//		Runnable r19 = new InsertionThread(18000000,19000000, dataPath + "/comment_hasCreator_person.csv",2,commentToPersonS19);
//		new Thread(r19).start(); 
//		Runnable r20 = new InsertionThread(19000000,99000000, dataPath + "/comment_hasCreator_person.csv",2,commentToPersonS20);
//		new Thread(r20).start(); 
		try
		{
			while(counter>0)
			{
				Thread.currentThread().sleep(300l);				
			}
//			commentToPersonS1.putAll(commentToPersonS2);
//			commentToPersonS1.putAll(commentToPersonS3);
//			commentToPersonS1.putAll(commentToPersonS4);
			//System.out.println(commentToPersonS1.keySet().size() );
			System.out.println("I am on: " + (System.currentTimeMillis()-time2));
//			Thread.currentThread().sleep(1000000000l);
		}
		catch (Exception ex)
		{
			ex.printStackTrace();
		}
		BufferedReader br = null; 
		try{
			//step 1 start
//			br = new BufferedReader(new FileReader(dataPath + "/comment_hasCreator_person.csv"));  
//			String line = null;  
//			br.readLine();
//			
//			HashMap<Integer, Integer> commentToPerson = new HashMap<Integer, Integer>(); 
//			long time1 = System.currentTimeMillis(); 
//			while ((line = br.readLine()) != null)  
//			{
//				StringTokenizer st = new StringTokenizer(line,"|");
//				Integer commentId = Integer.valueOf(st.nextToken());
//				Integer creatorId = Integer.valueOf(st.nextToken());
//				commentToPerson.put(commentId, creatorId);
//			}
//			System.out.println("Time to load comment_hasCreator_person: " + (System.currentTimeMillis()-time1));
			
			
			//step 1 end 
			//step 2 start 
//			br.close();
			br = new BufferedReader(new FileReader(dataPath + "/comment_replyOf_comment.csv"));  
			String line = null;  
			br.readLine();
			
			//counter that counts the number of comments replied 
			
			int counter = 0; 
						
			while ((line = br.readLine()) != null)  
			{
				StringTokenizer st = new StringTokenizer(line,"|");
				Integer replyId = Integer.valueOf(st.nextToken());
				Integer commentId = Integer.valueOf(st.nextToken());
				int id1=0; 
				int id2=0;
				if(replyId>=49999980&&replyId<99999980)
				{
					id1=1;
				}
				else if (replyId>=99999980&&replyId<149999980)
				{
					id1=2;
					
				}
				else if(replyId>=149999980)
				{
					id1=3;
				}
				if(commentId>=49999980&&commentId<99999980)
				{
					id2=1;
				}
				else if (commentId>=99999980&&commentId<149999980)
				{
					id2=2;
					
				}
				else if(commentId>=149999980)
				{
					id2=3;
				}
				Integer commentFrom = commentToPersonS1[id1].get(replyId);//.get(replyId); 
				Integer commentTo = commentToPersonS1[id2].get(commentId); //commentToPerson.get(commentId); 
				Comment comment = new Comment(); 
				comment.setUserIdFrom(commentFrom);
				comment.setUserIdTo(commentTo);
				
				
				Integer number = commentsConnectedPersons.get(comment);
				if(number==null || number<=comments)
				{
					if(number==null)
					{
						number=1; 
					}
					else
					{
						number++; 
					}
					commentsConnectedPersons.put(comment, number);
				}
			}
			
			
		}
		catch(Exception ex)
		{
			//do something about it? 
		}
		finally{
			try
			{
				if(br!=null)
				{
					br.close();
				}
			}
			catch(Exception ex)
			{
				
			}
		}
	}	
	
	public static void main(String [] args) throws Exception
	{
		LoaderQuery1 loadQuery1 = new LoaderQuery1();
//		HashMap<Integer,ArrayList<Integer>> testResult = 
//				loadQuery1.loadData(58,402,0,"/Users/klimzaporojets/klim/umass/CMPSCI645 Database Design and Implementation/project topics/social_networks/big_data_files");
		HashMap<Integer,ArrayList<Integer>> loadDataThreads = 
				loadQuery1.loadDataThreads(58,402,0,"/Users/klimzaporojets/klim/umass/CMPSCI645/project_topics/social_networks/big_data_files");
		
		System.out.println("The end");
	}
}
