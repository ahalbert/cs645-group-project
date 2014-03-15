package query1.loaders;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.StringTokenizer;

import query1.dtos.Comment;

/**
 * 
 * @author klimzaporojets
 * Loads data for query1 
 */
public class LoaderQuery1 {
	
	HashMap<Comment, Integer> commentsConnectedPersons = new HashMap<Comment, Integer>();
	
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
			doPreload(dataPath); 
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
			
			
			while ((line = br.readLine()) != null)  
			{
				StringTokenizer st = new StringTokenizer(line,"|");
				Integer commentId = Integer.valueOf(st.nextToken());
				Integer creatorId = Integer.valueOf(st.nextToken());
				commentToPerson.put(commentId, creatorId);
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
	public static void main(String [] args)
	{
		LoaderQuery1 loadQuery1 = new LoaderQuery1();
		HashMap<Integer,ArrayList<Integer>> testResult = loadQuery1.loadData(58,402,0,"/Users/klimzaporojets/klim/umass/CMPSCI645 Database Design and Implementation/project topics/social_networks/data_files");
		
		System.out.println("The end");
	}
}