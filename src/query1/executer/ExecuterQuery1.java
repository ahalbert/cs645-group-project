package query1.executer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;

import query1.dtos.Path;
import query1.indexer.LuceneIndexer;
import query1.indexer.MapDBIndexer;
import query1.loaders.IndexedLoaderQuery1;
import query1.loaders.LoaderQuery1;

import java.util.Queue;

/**
 * 
 * @author klimzaporojets
 *
 */
public class ExecuterQuery1 {
	
	//first version: breadth first search
	public void findPath(Integer userFrom, Integer userTo, Integer comments, String filePath)
	{
		LoaderQuery1 loaderQuery1 = new LoaderQuery1(); 
		HashMap<Integer,ArrayList<Integer>> data = null;
		
		
		long time = System.currentTimeMillis();
		
		data = loaderQuery1.loadData(userFrom, userTo, comments, filePath);
		
		System.out.println("Time to load the data: " + (System.currentTimeMillis()-time));
		
//		ArrayList<Integer> neighbours = data.get(userFrom);
		LinkedList<Integer> queue = new LinkedList<Integer>(); 
		queue.add(userFrom);
		
		HashMap<Integer,Path> distance = new HashMap<Integer,Path>();
		Path path = new Path(0,null);
		distance.put(userFrom, path);
		Integer currentElement=null;
		Integer previousElement=null;
		Integer previousDist=0;
		while(queue.size()>0 && !(currentElement=queue.poll()).equals(userTo))
		{
//			if(currentElement==402)
//			{
//				System.out.println("current element is 402");
//			}
			Path currentPath = distance.get(currentElement);
//			if(dist==null)
//			{
//				System.out.println("Error!!! distance should not be null!!!");
//			}
			ArrayList<Integer> neighbours = data.get(currentElement);
			
			if(neighbours!=null)
			{
				
				for(Integer neighbour:neighbours)
				{
					Path neighbourDistance = distance.get(neighbour); 
					if(neighbourDistance==null)
					{
						distance.put(neighbour, new Path(currentPath.getDistanceToOrigin()+1,currentElement));
						queue.add(neighbour);
					}
					else
					{
						//System.out.println("distance not null");
					}
				}
				
				previousElement=currentElement;
			}

		}
		Path parent = null;
		if(!currentElement.equals(userTo))
		{
			System.out.println("Path not found");
		}
		else
		{
			System.out.println(currentElement);
			while((parent=distance.get(currentElement)).getParent()!=null)
			{
				currentElement=parent.getParent(); 
				System.out.println(currentElement);
			}
		}
		System.out.println("The end 2a");
		
	}
	
	//second version: preloads the person_knows_person first 
	public void findPathV2(Integer userFrom, Integer userTo, Integer comments, String filePath)
	{
		LoaderQuery1 loaderQuery1 = new LoaderQuery1(); 
		HashMap<Integer,ArrayList<Integer>> data = null;
		
		
		long time = System.currentTimeMillis();
		
		data = loaderQuery1.loadDataV2(userFrom, userTo, comments, filePath);
		
//		System.out.println("Time to load the data: " + (System.currentTimeMillis()-time));
		
//		ArrayList<Integer> neighbours = data.get(userFrom);
		LinkedList<Integer> queue = new LinkedList<Integer>(); 
		queue.add(userFrom);
		
		HashMap<Integer,Path> distance = new HashMap<Integer,Path>();
		Path path = new Path(0,null);
		distance.put(userFrom, path);
		Integer currentElement=null;
		Integer previousElement=null;
		Integer previousDist=0;
		while(queue.size()>0 && !(currentElement=queue.poll()).equals(userTo))
		{
//			if(currentElement==402)
//			{
//				System.out.println("current element is 402");
//			}
			Path currentPath = distance.get(currentElement);
//			if(dist==null)
//			{
//				System.out.println("Error!!! distance should not be null!!!");
//			}
			ArrayList<Integer> neighbours = data.get(currentElement);
			
			if(neighbours!=null)
			{
				
				for(Integer neighbour:neighbours)
				{
					Path neighbourDistance = distance.get(neighbour); 
					if(neighbourDistance==null)
					{
						distance.put(neighbour, new Path(currentPath.getDistanceToOrigin()+1,currentElement));
						queue.add(neighbour);
					}
					else
					{
						//System.out.println("distance not null");
					}
				}
				
				previousElement=currentElement;
			}

		}
		Path parent = null;
		if(!currentElement.equals(userTo))
		{
			System.out.println("path none");
		}
		else
		{
			//System.out.println(currentElement);
			String output; 
			output = currentElement.toString();  
			while((parent=distance.get(currentElement)).getParent()!=null)
			{
				currentElement=parent.getParent();
				output = output + "-" + currentElement.toString();
//				System.out.println(currentElement);
			}
			System.out.println("path " + output); 
		}
//		System.out.println("The end 2a");
		
	}	

	//first version: breadth first search
	public void findPathWithIndex(Integer userFrom, Integer userTo, Integer comments)
	{
		IndexedLoaderQuery1 loaderQuery1 = new IndexedLoaderQuery1(); 
		LuceneIndexer luceneIndexer = new LuceneIndexer(); 
		long time = System.currentTimeMillis();
		loaderQuery1.doIndexPreload("/Users/klimzaporojets/klim/umass/CMPSCI645 Database "
				+ "Design and Implementation/project topics/social_networks/big_data_files");
		System.out.println("loading time: " + (System.currentTimeMillis()-time));
		
//		ArrayList<Integer> neighbours = data.get(userFrom);
		LinkedList<Integer> queue = new LinkedList<Integer>(); 
		queue.add(userFrom);
		
		HashMap<Integer,Path> distance = new HashMap<Integer,Path>();
		Path path = new Path(0,null);
		distance.put(userFrom, path);
		Integer currentElement=null;
		Integer previousElement=null;
		Integer previousDist=0;
		while(queue.size()>0 && !(currentElement=queue.poll()).equals(userTo))
		{
			Path currentPath = distance.get(currentElement);
			ArrayList<Integer> neighbours = luceneIndexer.getUsersConnected(currentElement, IndexedLoaderQuery1.indexPersonKnowsPerson);
			
			if(neighbours!=null)
			{
				
				for(Integer neighbour:neighbours)
				{
					Path neighbourDistance = distance.get(neighbour);
					Boolean isEnoughComments = luceneIndexer.isEnoughComments(currentElement, neighbour, comments, IndexedLoaderQuery1.indexCommentsPath);
					if(neighbourDistance==null&&isEnoughComments)
					{
						distance.put(neighbour, new Path(currentPath.getDistanceToOrigin()+1,currentElement));
						queue.add(neighbour);
					}
					else
					{
						//System.out.println("distance not null");
					}
				}
				
				previousElement=currentElement;
			}

		}
		Path parent = null;
		if(!currentElement.equals(userTo))
		{
			System.out.println("Path not found");
		}
		else
		{
			System.out.println(currentElement);
			while((parent=distance.get(currentElement)).getParent()!=null)
			{
				currentElement=parent.getParent(); 
				System.out.println(currentElement);
			}
		}
		System.out.println("The end 2a");
		
	}
	
	
	//first version: breadth first search
	public void findPathWithIndexBTree(Integer userFrom, Integer userTo, Integer comments, 
			String datapath) throws Exception
	{ 
		MapDBIndexer mapDbIndexer = new MapDBIndexer();
		long timeLoad = System.currentTimeMillis();
		mapDbIndexer.Index(datapath);
		System.out.println("total time to load: " + (System.currentTimeMillis()-timeLoad));
		
		
		long timeSearch = 0; 
		timeSearch = System.currentTimeMillis(); 
//		int neighboursProcessed = 0; 
		LinkedList<Integer> queue = new LinkedList<Integer>(); 
		queue.add(userFrom);
		
		HashMap<Integer,Path> distance = new HashMap<Integer,Path>();
		Path path = new Path(0,null);
		distance.put(userFrom, path);
		Integer currentElement=null;
		Integer previousElement=null;
		Integer previousDist=0;
		while(queue.size()>0 && !(currentElement=queue.poll()).equals(userTo))
		{
			Path currentPath = distance.get(currentElement);
			ArrayList<Integer> neighbours = mapDbIndexer.getUsersConnected(currentElement, datapath);
			
			if(neighbours!=null)
			{
				
				for(Integer neighbour:neighbours)
				{
					Path neighbourDistance = distance.get(neighbour);
//					Boolean isEnoughComments = luceneIndexer.isEnoughComments(currentElement, neighbour, comments, IndexedLoaderQuery1.indexCommentsPath);
					if(neighbourDistance==null/*&&isEnoughComments*/)
					{
						Boolean isEnoughComments = mapDbIndexer.isEnoughComments(currentElement, neighbour, comments, datapath);
//						neighboursProcessed++; 
//						if(neighboursProcessed%100==0)
//						{
//							System.out.println("Neighbours processed: " + neighboursProcessed);
//						}
						
						if(isEnoughComments)
						{
							distance.put(neighbour, new Path(currentPath.getDistanceToOrigin()+1,currentElement));
							queue.add(neighbour);
						}
					}
					else
					{
						//System.out.println("distance not null");
					}
				}
				
				previousElement=currentElement;
			}

		}
		Path parent = null;
		if(!currentElement.equals(userTo))
		{
			System.out.println("Path not found");
		}
		else
		{
			System.out.println(currentElement);
			while((parent=distance.get(currentElement)).getParent()!=null)
			{
				currentElement=parent.getParent(); 
				System.out.println(currentElement);
			}
		}
		
		long timeRes = (System.currentTimeMillis() - timeSearch);
		System.out.println("Total time to search: " + timeRes);
	}	
	
	
	//first version: breadth first search, calculating some stuff "on the fly" 
	public void findPathWithIndexBTreeHybrid(Integer userFrom, Integer userTo, Integer comments, 
			String datapath) throws Exception
	{ 
		MapDBIndexer mapDbIndexer = new MapDBIndexer();
		long timeLoad = System.currentTimeMillis();
		
		
		mapDbIndexer.HybridIndex(datapath);
		System.out.println("total time to load: " + (System.currentTimeMillis()-timeLoad));
		long timeSearch = 0; 
		timeSearch = System.currentTimeMillis(); 
//		int neighboursProcessed = 0; 
		
		LinkedList<Integer> queue = new LinkedList<Integer>(); 
		queue.add(userFrom);
		
		HashMap<Integer,Path> distance = new HashMap<Integer,Path>();
		Path path = new Path(0,null);
		distance.put(userFrom, path);
		Integer currentElement=null;
		Integer previousElement=null;
		Integer previousDist=0;
		while(queue.size()>0 && !(currentElement=queue.poll()).equals(userTo))
		{
			Path currentPath = distance.get(currentElement);
			ArrayList<Integer> neighbours = mapDbIndexer.getUsersConnected(currentElement, datapath);
			
			if(neighbours!=null)
			{
				
				for(Integer neighbour:neighbours)
				{
					Path neighbourDistance = distance.get(neighbour);
//					Boolean isEnoughComments = luceneIndexer.isEnoughComments(currentElement, neighbour, comments, IndexedLoaderQuery1.indexCommentsPath);
					if(neighbourDistance==null/*&&isEnoughComments*/)
					{
						Boolean isEnoughComments = mapDbIndexer.isEnoughCommentsHybrid(currentElement, neighbour, comments, datapath);
//						neighboursProcessed++; 
//						if(neighboursProcessed%100==0)
//						{
//							System.out.println("Neighbours processed: " + neighboursProcessed);
//						}
						if(isEnoughComments)
						{
							distance.put(neighbour, new Path(currentPath.getDistanceToOrigin()+1,currentElement));
							queue.add(neighbour);
						}
					}
					else
					{
						//System.out.println("distance not null");
					}
				}
				
				previousElement=currentElement;
			}

		}
		Path parent = null;
		if(!currentElement.equals(userTo))
		{
			System.out.println("Path not found");
		}
		else
		{
			System.out.println(currentElement);
			while((parent=distance.get(currentElement)).getParent()!=null)
			{
				currentElement=parent.getParent(); 
				System.out.println(currentElement);
			}
		}
		System.out.println("Total time to search: " + (System.currentTimeMillis()-timeSearch));
		
	}		
	
	public static void main(String [] args) throws Exception
	{
		
		ExecuterQuery1 executerQuery1 = new ExecuterQuery1(); 
//		executerQuery1.findPath(58,402,2);
//		executerQuery1.findPath(858,587,1);
//		executerQuery1.findPathWithIndex(858, 587, 1);
		long time = System.currentTimeMillis();
		//executerQuery1.findPathWithIndexBTree(858, 58700, 1);
		//executerQuery1.findPath(858, 58700, 1);91851
//		executerQuery1.findPathV2(858, 587, 1, 
//				"/Users/klimzaporojets/klim/umass/CMPSCI645/project_topics/social_networks/big_data_files");
		executerQuery1.findPathV2(858, 587, 1, 
				"/Users/klimzaporojets/klim/umass/CMPSCI645/project_topics/social_networks/big_data_files");
//		executerQuery1.findPathWithIndexBTreeHybrid(858, 587, 1, 
//				"/Users/klimzaporojets/klim/umass/CMPSCI645 Database "
//				+ "Design and Implementation/project topics/social_networks/data_files");

//		System.out.println(System.currentTimeMillis()-time);
//		System.out.println("The end 2");
	}
}
