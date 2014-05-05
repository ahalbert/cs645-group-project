package query1.loaders;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.StringTokenizer;

import query1.dtos.Comment;
import query1.indexer.LuceneIndexer;

/**
 * 
 * @author klimzaporojets
 * Loads data for query1 
 */
public class IndexedLoaderQuery1 {
	
	HashMap<Comment, Integer> commentsConnectedPersons = new HashMap<Comment, Integer>();
	//by default index does not exist 
	public static boolean indexCommentHasCreatorPerson = false;  
	
	public static String indexCommentsPath = "/Users/klimzaporojets/klim/umass/CMPSCI645 Database Design and Implementation/project topics/social_networks/index_comments_10k"; 
	public static String indexPersonCreatorCommentsPath = "/Users/klimzaporojets/klim/umass/CMPSCI645 Database Design and Implementation/project topics/social_networks/index_person_creator_10k"; 
	public static String indexPersonKnowsPerson = "/Users/klimzaporojets/klim/umass/CMPSCI645 Database Design and Implementation/project topics/social_networks/index_person_knows_person_10k"; 
	
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
					//here, save to index instead of in memory 
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
	
	//here check the index
	public boolean isNumberOfCommentsGreaterThanV2(Integer knowsFrom, Integer knowsTo, Integer numberOfComments, String dataPath)
	{
		//step 0: pre-loads all the data (only if not preloaded before)
		//step 1: gets the comments of knowsTo from comment_hasCreator_person
		//step 2: starts counting the answers of knowsFrom checking if these answers are for comments read in step 1. 
		//this is done reading the file comment_replyOf_comment
		if(!doesIndexExists(indexCommentsPath))
		{
			doIndexPreload(dataPath); 
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
	
	public boolean doesIndexExists(String path)
	{
		if(indexCommentHasCreatorPerson==true)
		{
			return true; 
		}
		else
		{
			return LuceneIndexer.isIndexCreated(path);
		}
	}
	public void doIndexPreload(String dataPath)
	{
		
		BufferedReader br = null; 
		try{
			//step 1 start
			
			LuceneIndexer luceneIndexer = new LuceneIndexer(); 
//			luceneIndexer.indexFile("/Users/klimzaporojets/klim/umass/CMPSCI645 Database Design and Implementation/project"
//					+ " topics/social_networks/big_data_files/person_knows_person.csv", indexPersonKnowsPerson,
//						"|", new String[]{"person_from","person_to"},false);
			
			luceneIndexer.indexFile(dataPath + "/person_knows_person.csv", indexPersonKnowsPerson,
						"|", new String[]{"person_from","person_to"},false);

			if(luceneIndexer.isIndexCreated(this.indexCommentsPath))
			{
				return ; 
			}
			br = new BufferedReader(new FileReader(dataPath + "/comment_hasCreator_person.csv"));  
			String line = null;  
			br.readLine();
						
			HashMap<Integer, Integer> commentToPerson = new HashMap<Integer, Integer>(); 
			
			int i =0;
			while ((line = br.readLine()) != null)  
			{
				if(i++%10000==0)
				{
					System.out.println(i);
				}
				StringTokenizer st = new StringTokenizer(line,"|");
				Integer commentId = Integer.valueOf(st.nextToken());
				Integer creatorId = Integer.valueOf(st.nextToken());
				commentToPerson.put(commentId, creatorId);
			}
						

			
			//counter that counts the number of comments replied 
			
			int counter = 0; 
			luceneIndexer.indexQuery1V2(indexPersonCreatorCommentsPath, indexCommentsPath, dataPath + "/comment_replyOf_comment.csv", commentToPerson);

			
			
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
	public static void main(String [] args)
	{
		LuceneIndexer luceneIndexer = new LuceneIndexer(); 
		
		luceneIndexer.indexFile("/Users/klimzaporojets/klim/umass/CMPSCI645 Database Design and Implementation/project"
				+ " topics/social_networks/big_data_files/person_knows_person.csv", indexPersonKnowsPerson,
					"|", new String[]{"person_from","person_to"},false);
		
		IndexedLoaderQuery1 loadQuery1 = new IndexedLoaderQuery1();
		//HashMap<Integer,ArrayList<Integer>> testResult = loadQuery1.loadData(58,402,0,"/Users/klimzaporojets/klim/umass/CMPSCI645 Database Design and Implementation/project topics/social_networks/data_files");
		loadQuery1.doIndexPreload("/Users/klimzaporojets/klim/umass/CMPSCI645 Database Design and Implementation/project topics/social_networks/big_data_files");
		
		System.out.println("The end");
	}
}
