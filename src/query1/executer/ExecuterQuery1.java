package query1.executer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;

import query1.dtos.Path;
import query1.loaders.LoaderQuery1;

import java.util.Queue;

/**
 * 
 * @author klimzaporojets
 *
 */
public class ExecuterQuery1 {
	
	//first version: breadth first search
	public void findPath(Integer userFrom, Integer userTo, Integer comments)
	{
		LoaderQuery1 loaderQuery1 = new LoaderQuery1(); 
		HashMap<Integer,ArrayList<Integer>> data = null;
		data = loaderQuery1.loadData(userFrom, userTo, comments, "/Users/klimzaporojets/klim/umass/CMPSCI645 Database "
				+ "Design and Implementation/project topics/social_networks/big_data_files");
		
		
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
	
	public static void main(String [] args)
	{
		
		ExecuterQuery1 executerQuery1 = new ExecuterQuery1(); 
		executerQuery1.findPath(58,402,4);
		System.out.println("The end 2");
	}
}
