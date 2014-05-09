package query1.loaders;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
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
					   if(counter==1)
					   {
						   counter++;
						   line = br.readLine();
					   }
					   if(!flag)
					   {
						   flag=true; 
						   //System.out.println(id + " " + line);
						   StringTokenizer st = new StringTokenizer(line,"|");
						   Integer commentId = Integer.valueOf(st.nextToken());						   
						   LoaderQuery1.limits[id] =  commentId;
					   }
						StringTokenizer st = new StringTokenizer(line,"|");
						Integer commentId = Integer.valueOf(st.nextToken());
						Integer creatorId = Integer.valueOf(st.nextToken());
						commentToPersonS.put(commentId, creatorId);
				   }
			   }
			 //  System.out.println("ready " + counter);
			   LoaderQuery1.counter--; 
		   }catch(Exception ex)
		   {
			   ex.printStackTrace();
		   }
	   }
	}

class InsertionThreadCommentsPerson implements Runnable {
	int from; 
	int to; 
	String path; 
	int id; 
	int commentsNumber; 
	HashSet<Comment> personKnowsPerson; 
//	boolean flag = false; 
	   public InsertionThreadCommentsPerson(int from, int to, String path, int id, int commentsNumber, 
			   HashSet<Comment> personKnowsPerson) {
	       // store parameter for later user
		   this.from = from; 
		   this.to = to; 
		   this.path = path; 
		   this.id = id; 
		   this.commentsNumber = commentsNumber; 
		   this.personKnowsPerson = personKnowsPerson; 
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
					   	if(counter==1)
					   	{
					   		counter++;
					   		line = br.readLine();
					   	}
						StringTokenizer st = new StringTokenizer(line,"|");
						Integer replyId = Integer.valueOf(st.nextToken());
						Integer commentId = Integer.valueOf(st.nextToken());
						int id1=0; 
						int id2=0;
						if(replyId>=LoaderQuery1.limits[1]&&replyId<LoaderQuery1.limits[2])
						{
							id1=1;
						}
						else if (replyId>=LoaderQuery1.limits[2]&&replyId<LoaderQuery1.limits[3])
						{
							id1=2;
							
						}
						else if(replyId>=LoaderQuery1.limits[3])
						{
							id1=3;
						}
						if(commentId>=LoaderQuery1.limits[1]&&commentId<LoaderQuery1.limits[2])
						{
							id2=1;
						}
						else if (commentId>=LoaderQuery1.limits[2]&&commentId<LoaderQuery1.limits[3])
						{
							id2=2;
							
						}
						else if(commentId>=LoaderQuery1.limits[3])
						{
							id2=3;
						}
						Integer commentFrom = LoaderQuery1.commentToPersonS1[id1].get(replyId);//.get(replyId); 
						Integer commentTo = LoaderQuery1.commentToPersonS1[id2].get(commentId); //commentToPerson.get(commentId); 
						Comment comment = new Comment(commentFrom,commentTo);
						
						if(//personKnowsPerson==null || 
								personKnowsPerson.contains(comment))
						{
							synchronized(LoaderQuery1.commentsConnectedPersons){
								Integer number = LoaderQuery1.commentsConnectedPersons.get(comment);
								if(number==null || number<=commentsNumber)
								{
									if(number==null)
									{
										number=1; 
									}
									else
									{
										number++; 
									}
								}
								LoaderQuery1.commentsConnectedPersons.put(comment, number);
							}
						}
				   }
			   }
			 //  System.out.println("ready " + counter);
			   LoaderQuery1.counter2--; 
		   }catch(Exception ex)
		   {
			   ex.printStackTrace();
		   }
	   }
	}

public class LoaderQuery1 {
	
	
	public static HashMap<Comment, Integer> commentsConnectedPersons = new HashMap<Comment, Integer>();
	public static int counter = 4;
	public static int counter2 = 4;
	public static Comment currentComment = null; 
	public static int limits[] = new int[counter]; 
//	public static ConcurrentHashMap<Integer, Integer> commentToPersonS = new ConcurrentHashMap<Integer, Integer>();
	public static HashMap<Integer,Integer> commentToPersonS1[] = new HashMap[4];
	
	//contains approximate ratios of rownumber/filesize for each of the files 
	public static HashMap<String,Float> filesToRatio = new HashMap<String,Float>(); 
	
	static{
		commentToPersonS1[0] = new HashMap<Integer,Integer>();
		commentToPersonS1[1] = new HashMap<Integer,Integer>();
		commentToPersonS1[2] = new HashMap<Integer,Integer>();
		commentToPersonS1[3] = new HashMap<Integer,Integer>();
		filesToRatio.put("comment_hasCreator_person.csv", 0.06976551998741f);
		filesToRatio.put("comment_replyOf_comment.csv", 0.05292565818127f);		
	}
	
	
	
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
	//V2 preloads person_knows_person before. 
	public HashMap<Integer,ArrayList<Integer>> loadDataV2(Integer from, Integer to, Integer numberOfComments, String dataPath)
	{
		HashSet<Comment> hashComments = new HashSet<Comment>();
		
		try
		{
//			long time = System.currentTimeMillis();
//			File f = new File(dataPath + "/comment_hasCreator_person.csv");
//			System.out.println(f.length());
//			System.out.println("length in: " + (System.currentTimeMillis()-time));
			
			BufferedReader br = new BufferedReader(new FileReader(dataPath + "/person_knows_person.csv"));  
			String line = null;  
			br.readLine(); 
			while ((line = br.readLine()) != null)  
			{
				StringTokenizer st = new StringTokenizer(line,"|");
				Integer knowsFrom = Integer.valueOf(st.nextToken());
				Integer knowsTo = Integer.valueOf(st.nextToken());
				Comment cmt = new Comment(knowsFrom, knowsTo); 
				hashComments.add(cmt);
			}
		}
		catch(Exception ex)
		{
			ex.printStackTrace();
		}
		HashMap<Integer, ArrayList<Integer>> query1Data = new HashMap<Integer, ArrayList<Integer>>();
		try
		{
			long time = System.currentTimeMillis(); 
//			BufferedReader br = new BufferedReader(new FileReader(dataPath + "/person_knows_person.csv"));  
//			String line = null;  
//			br.readLine(); 
//			while ((line = br.readLine()) != null)  
//			{
			Iterator<Comment> iterator = hashComments.iterator();
			while(iterator.hasNext()){
				//StringTokenizer may be much faster: 
				//http://www.javamex.com/tutorials/regular_expressions/splitting_tokenisation_performance.shtml#.UxoFNV6YS18
//				StringTokenizer st = new StringTokenizer(line,"|");
				Comment cmt = iterator.next();
				Integer knowsFrom = cmt.getUserIdFrom();
				Integer knowsTo = cmt.getUserIdTo();
				
				/*begin: check the number of comments*/
				boolean doBelong = false; 
				if(numberOfComments==-1)
				{
					doBelong=true; 
				}
				else
				{
					doBelong = isNumberOfCommentsGreaterThanV3(knowsFrom,knowsTo,numberOfComments, dataPath, 
							hashComments);
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
//			br.close();
//			System.out.println("reading straight from file: " + (System.currentTimeMillis()-time));
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
			doPreload(dataPath); 
			//doPreloadImproved(dataPath, numberOfComments); 
		}
		Comment comment = new Comment(knowsFrom, knowsTo); 
//		comment.setUserIdFrom(knowsFrom);
//		comment.setUserIdTo(knowsTo);
		
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
	//uses info from person_knows_person in order no to examine more comment_Replyof_comment than necessary
	public boolean isNumberOfCommentsGreaterThanV3(Integer knowsFrom, Integer knowsTo, 
			Integer numberOfComments, String dataPath, HashSet<Comment> personKnowsPerson)
	{
		//step 0: pre-loads all the data (only if not preloaded before)
		//step 1: gets the comments of knowsTo from comment_hasCreator_person
		//step 2: starts counting the answers of knowsFrom checking if these answers are for comments read in step 1. 
		//this is done reading the file comment_replyOf_comment
		if(commentsConnectedPersons.size()==0)
		{
			//doPreload(dataPath); 
			doPreloadImprovedV2(dataPath, numberOfComments, personKnowsPerson); 
		}
		Comment comment = new Comment(knowsFrom, knowsTo); 
//		comment.setUserIdFrom(knowsFrom);
//		comment.setUserIdTo(knowsTo);
		
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
				Comment comment = new Comment(commentFrom, commentTo); 
//				comment.setUserIdFrom(commentFrom);
//				comment.setUserIdTo(commentTo);
				
				
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
	public void doPreloadImprovedV2(String dataPath, int comments, 
				HashSet<Comment> personKnowsPerson)
	{
		long time2 = System.currentTimeMillis();
		File f = new File(dataPath + "/comment_hasCreator_person.csv"); 
		long fileSize =  f.length(); 
		float ratio = filesToRatio.get("comment_hasCreator_person.csv");
		int lines = Math.round(fileSize*ratio); 
		int range = lines/4; 
		
		Runnable r = new InsertionThread(1,range*1, dataPath + "/comment_hasCreator_person.csv",0,commentToPersonS1[0]);
		Thread t = new Thread(r); 
		t.setPriority(Thread.MAX_PRIORITY);
		t.start();
		Runnable r2 = new InsertionThread(range*1,range*2, dataPath + "/comment_hasCreator_person.csv",1,commentToPersonS1[1]);
		Thread t2 = new Thread(r2); 
		t2.setPriority(Thread.MAX_PRIORITY);
		t2.start();
		Runnable r3 = new InsertionThread(range*2,range*3, dataPath + "/comment_hasCreator_person.csv",2,commentToPersonS1[2]);
		Thread t3 = new Thread(r3); 
		t3.setPriority(Thread.MAX_PRIORITY);
		t3.start();
		Runnable r4 = new InsertionThread(range*3,99999999, dataPath + "/comment_hasCreator_person.csv",3,commentToPersonS1[3]);
		Thread t4 = new Thread(r4); 
		t4.setPriority(Thread.MAX_PRIORITY);
		t4.start();

		try
		{
			while(counter>0)
			{
				Thread.currentThread().sleep(10l);				
			}
	//		System.out.println("I am on: " + (System.currentTimeMillis()-time2));
		}
		catch (Exception ex)
		{
			ex.printStackTrace();
		}
		BufferedReader br = null; 
		try{
			//step 1 start
			f = new File(dataPath + "/comment_replyOf_comment.csv"); 
			fileSize =  f.length(); 
			ratio = filesToRatio.get("comment_replyOf_comment.csv");
			lines = Math.round(fileSize*ratio); 
			range = lines/4; 

			//InsertionThreadCommentsPerson
			time2 = System.currentTimeMillis(); 
			Runnable rs = new InsertionThreadCommentsPerson(1,range*1, dataPath + "/comment_replyOf_comment.csv",0,comments, personKnowsPerson);
			Thread ts = new Thread(rs); 
			ts.setPriority(Thread.MAX_PRIORITY);
			ts.start();
			Runnable rs2 = new InsertionThreadCommentsPerson(range*1,range*2, dataPath + "/comment_replyOf_comment.csv",1,comments, personKnowsPerson);
			Thread ts2 = new Thread(rs2); 
			ts2.setPriority(Thread.MAX_PRIORITY);
			ts2.start();
			Runnable rs3 = new InsertionThreadCommentsPerson(range*2,range*3, dataPath + "/comment_replyOf_comment.csv",2,comments, personKnowsPerson);
			Thread ts3 = new Thread(rs3); 
			ts3.setPriority(Thread.MAX_PRIORITY);
			ts3.start();
			Runnable rs4 = new InsertionThreadCommentsPerson(range*3,99999999, dataPath + "/comment_replyOf_comment.csv",3,comments, personKnowsPerson);
			Thread ts4 = new Thread(rs4); 
			ts4.setPriority(Thread.MAX_PRIORITY);
			ts4.start();

			try
			{
				while(counter2>0)
				{
					Thread.currentThread().sleep(10l);				
				}
	//			System.out.println("I am on 2: " + (System.currentTimeMillis()-time2));
			}
			catch (Exception ex)
			{
				ex.printStackTrace();
			}
		}
		catch(Exception ex)
		{
			//do something about it?
			ex.printStackTrace();
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
				ex.printStackTrace();
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

		try
		{
			while(counter>0)
			{
				Thread.currentThread().sleep(10l);				
			}
			System.out.println("I am on: " + (System.currentTimeMillis()-time2));
		}
		catch (Exception ex)
		{
			ex.printStackTrace();
		}
		BufferedReader br = null; 
		try{
			//step 1 start

			//InsertionThreadCommentsPerson
			time2 = System.currentTimeMillis(); 
			Runnable rs = new InsertionThreadCommentsPerson(1,3000000, dataPath + "/comment_replyOf_comment.csv",0,comments,null);
			Thread ts = new Thread(rs); 
			ts.setPriority(Thread.MAX_PRIORITY);
			ts.start();
			Runnable rs2 = new InsertionThreadCommentsPerson(3000000,6000000, dataPath + "/comment_replyOf_comment.csv",1,comments,null);
			Thread ts2 = new Thread(rs2); 
			ts2.setPriority(Thread.MAX_PRIORITY);
			ts2.start();
			Runnable rs3 = new InsertionThreadCommentsPerson(6000000,9000000, dataPath + "/comment_replyOf_comment.csv",2,comments,null);
			Thread ts3 = new Thread(rs3); 
			ts3.setPriority(Thread.MAX_PRIORITY);
			ts3.start();
			Runnable rs4 = new InsertionThreadCommentsPerson(9000000,99999999, dataPath + "/comment_replyOf_comment.csv",3,comments,null);
			Thread ts4 = new Thread(rs4); 
			ts4.setPriority(Thread.MAX_PRIORITY);
			ts4.start();

			try
			{
				while(counter2>0)
				{
					Thread.currentThread().sleep(10l);				
				}
				System.out.println("I am on 2: " + (System.currentTimeMillis()-time2));
			}
			catch (Exception ex)
			{
				ex.printStackTrace();
			}
		}
		catch(Exception ex)
		{
			//do something about it?
			ex.printStackTrace();
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
				ex.printStackTrace();
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
