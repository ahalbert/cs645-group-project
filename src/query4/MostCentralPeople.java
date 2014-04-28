package query4;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.LinkedList;
import java.util.Queue;
import java.util.StringTokenizer;
import java.util.TreeMap;

import query2.Person;

public class MostCentralPeople {
	
	static String tagNames = "tag.csv";
	static String forumTagFile = "forum_hasTag_tag.csv";
	static String forumMemberFile = "forum_hasMember_person.csv";
	static String friendshipFile = "person_knows_person.csv";
	
	String dir;
	Queue<String[]> queries;
	
	/*
	 * 
	 * Read queries and set them in a Queue 
	 * (Possible optimization: group same forum name queries) 
	 * 
	 */
	public MostCentralPeople(String dataDir, String queryF){		
		queries = new LinkedList<String[]>();
		this.dir = dataDir;
		// Code below was stolen from Armand
		try {
            BufferedReader file = new BufferedReader(new FileReader(dataDir + queryF));
            String s;
            while (( s = file.readLine() ) != null)  {
                s = s.substring(7, s.length()-1);
                s = s.replaceAll("\\s","");
                String[] values = s.split(",");
                this.queries.add(values);
            }
            file.close();
        } catch (IOException e) { e.printStackTrace(); }
		System.out.println(this.queries.size() + " queries4 loaded.");
	}
	
	
	/*
	 * 
	 * For each query create the graph and output the n most central people
	 * (Possible optimization: use the same graph for queries with same tag)
	 * 
	 */
	public void naiveApproach(){
		System.out.println("Naive approach:");
		long startTime = System.currentTimeMillis();
		while(this.queries.isEmpty() == false){
			String[] currentQ = this.queries.remove();
			String currentTag = currentQ[1];
			int k = Integer.valueOf(currentQ[0]);
			//System.out.println("Forum tag: " + currentTag);
			ArrayList<CentralPerson> currentG = this.naiveGraph(currentTag);
			currentG = this.computeCentralities(currentG);
			// First order by ID (needed to break centrality ties)
			Collections.sort(currentG, new CentralPersonComp(false));
			// Now sort by centrality
			Collections.sort(currentG, new CentralPersonComp(true));
			for(int i = 0; i < k; i++){
				System.out.print(currentG.get(i).getId() + " ");
			}
			System.out.print("% centrality values ");
			for(int i = 0; i < k; i++){
				System.out.print(currentG.get(i).getCentrality() + " ");
			}
			System.out.println();
		}
		System.out.println("Chrono read: " + (System.currentTimeMillis() - startTime) + "ms");
	}
	
	/*
	 * 
	 * Read the csv files to form the graph:
	 * 1. forum has tag to find the right forums
	 * 2. forum has member to find all members of that forum. 
	 * For each member, create a Central Person object and add it to the graph
	 * 3. person knows person to fill the friendship tables of our CentralPersons
	 * For each entry a|b, add b to a's friend list
	 * (Possible optimization: add a to b's friend list and find a way to ignore redundant csv entries)
	 * 
	 */
	private ArrayList<CentralPerson> naiveGraph(String forumTag){
		ArrayList<CentralPerson> graph = new ArrayList<CentralPerson>();
		ArrayList<Integer> concernedForums = new ArrayList<Integer>();
		ArrayList<Integer> concernedPeople = new ArrayList<Integer>();
		int tagID = 0;
		
		//Find the tag ID for the give forumTag
		try {
            BufferedReader file = new BufferedReader(new FileReader(this.dir + tagNames));
            String s;
            boolean go = true;
            // Ignore the first line
            file.readLine();
            while (( s = file.readLine() ) != null && go)  {
            	StringTokenizer st = new StringTokenizer(s,"|");
				Integer t  = Integer.valueOf(st.nextToken());
				String tname = st.nextToken();
				if(tname.equals(forumTag)){
					tagID = t;
					go = false;
				}
            }
            file.close();
        } catch (IOException e) { e.printStackTrace(); }
		
		//Find concerned forums
		try {
            BufferedReader file = new BufferedReader(new FileReader(this.dir + forumTagFile));
            String s;
            // Ignore the first line
            file.readLine();
            while (( s = file.readLine() ) != null)  {
            	StringTokenizer st = new StringTokenizer(s,"|");
				Integer fId = Integer.valueOf(st.nextToken());
				Integer tId = Integer.valueOf(st.nextToken());
				if(tId == tagID){
					concernedForums.add(fId);
				}
            }
            file.close();
        } catch (IOException e) { e.printStackTrace(); }
		
		//Find concerned people
		try {
            BufferedReader file = new BufferedReader(new FileReader(this.dir + forumMemberFile));
            String s;
            // Ignore the first line
            file.readLine();
            while (( s = file.readLine() ) != null)  {
            	StringTokenizer st = new StringTokenizer(s,"|");
				Integer fId = Integer.valueOf(st.nextToken());
				Integer pId = Integer.valueOf(st.nextToken());
				// Making sure to check for duplicate people of course!
				if(concernedForums.contains(fId) && !concernedPeople.contains(pId)){
					concernedPeople.add(pId);
				}
            }
            file.close();
        } catch (IOException e) { e.printStackTrace(); }
		
		// System.out.println(concernedPeople.size() + " people concerned.");
		
		//Add concerned people to the graph
		for(Integer id : concernedPeople){
			graph.add(new CentralPerson(id));
		}
	
		
		//Populate friendship arrays
		try {
            BufferedReader file = new BufferedReader(new FileReader(this.dir + friendshipFile));
            String s;
            // Ignore the first line
            file.readLine();
            while (( s = file.readLine() ) != null)  {
            	StringTokenizer st = new StringTokenizer(s,"|");
				Integer pId1 = Integer.valueOf(st.nextToken());
				Integer pId2 = Integer.valueOf(st.nextToken());
				if(concernedPeople.contains(pId1) && concernedPeople.contains(pId2)){
					for(CentralPerson p : graph){
						if(p.getId() == pId1){
							p.addFriend(pId2);
						}
						if(p.getId() == pId2){
							p.addFriend(pId1);
						}
					}
				}
            }
            file.close();
        } catch (IOException e) { e.printStackTrace(); }
		
		return graph;
	}
	
	/*
	 * Using edges, compute the initial adjacency matrix of the graph
	 * (Possible optimization: because it's undirected, we could do half the work)
	 */
	private int[][] computeInitialMatrix(ArrayList<CentralPerson> graph){
		int n = graph.size();
		int[][] initialMatrix = new int[n][n];
		for(int i = 0; i < n; i++){
			for(int j = 0; j < n; j++){
				if(graph.get(i).knows(graph.get(j).getId())){
					initialMatrix[i][j] = 1;
				}
				else if(i == j){
					initialMatrix[i][j] = 0;
				}
				else{
					// This is longer than any shortest path can be
					initialMatrix[i][j] = 2 * n;
				}
			}
		}
		return initialMatrix;
	}
	
	/*
	 * Computes the shortest path matrix using the initial adjacency and FloydWarshall
	 */
	private int[][] shortestPaths(int[][] initialMatrix){
		int n = initialMatrix.length;
		for(int i = 0; i < n; i++){
			for(int j = 0; j < n; j++){
				for(int k = 0; k < n; k++){
					initialMatrix[i][j] = Math.min(initialMatrix[i][j], initialMatrix[i][k] + initialMatrix[k][j]);
				}
			}
		}
		return initialMatrix;
	}
	
	/*
	 * Computes the centrality of the people of a graph
	 */
	private ArrayList<CentralPerson> computeCentralities(ArrayList<CentralPerson> graph){
		int[][] minDist = this.shortestPaths(this.computeInitialMatrix(graph));
		//this.printMatrix(minDist);
		for(int i = 0; i < graph.size(); i++){
			CentralPerson p = graph.get(i);
			float reach = 0;
			float s = 0;
			for(Integer d : minDist[i]){
				if(d < graph.size()){
					s += d;
					reach +=1;
				}
			}
			reach -= 1;
			float f;
			if ((graph.size() -1) * s != 0){
				f = (reach * reach) / ((graph.size() -1) * s);
			}
			else{
				f = 0;
			}
			p.setCentrality(f);
		}
		return graph;
	
	}
	
	/*
	 * Helpers
	 */
	private void printMatrix(int[][] m){
		for(int i = 0; i < m.length; i++){
			for(int j = 0; j < m.length; j++){
				System.out.print("[" + m[i][j] + "]");
			}
			System.out.println();
		}		
	}
	

	
	
	
	

}
