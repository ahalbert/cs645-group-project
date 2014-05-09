package query3;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.Queue;
import java.util.StringTokenizer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.TreeMap;


public class Socializer {

	static String personInterests = "person_hasInterest_tag.csv";
	static String personPlaceLoc = "person_isLocatedIn_place.csv";
	static String personStudy = "person_studyAt_organisation.csv";
	static String personWork = "person_workAt_organisation.csv";
	static String organisationLoc = "organisation_isLocatedIn_place.csv";
	static String placePart = "place_isPartOf_place.csv";
	static String placeFile = "place.csv";
	static String friendshipFile = "person_knows_person.csv";
	
	private long lTime, eTime;
	
	
	
	HashSet<Integer> allConcernedPeople;
	ArrayList<Integer> allPeople;
	String dir;
	Queue<String []> queries;
	HashMap<Integer, Place> placeMap = null;
	HashMap<String, HashSet<Integer>> peoplePerPlace = null;
	
	
	/*
	 * Naives approach attributes
	 */
	HashMap<Integer, SocialPerson> nGraph;
	
	/*
	 * Optimized approach attributes
	 */
	TreeMap<String, SocialPerson> oGraph;

	
	ArrayList<Integer> debugId;
	
	/* 
	 * Initialize without queries and feed them one by one.
	 */
	public Socializer(){		
		queries = new LinkedList<String[]>();
		allPeople = new ArrayList<Integer>();
		allConcernedPeople = new HashSet<Integer>();
	}
	
	public void addQuery(String [] q){
		queries.add(q);
	}
	
	public Socializer(String dataDir, String queryF){		
		queries = new LinkedList<String[]>();
		this.dir = dataDir;
		lTime = eTime = 0;
		long sLT = System.currentTimeMillis();
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
		lTime += System.currentTimeMillis() - sLT;
		System.out.println(this.queries.size() + " queries3 loaded.");
		
		
		allPeople = new ArrayList<Integer>();
		allConcernedPeople = new HashSet<Integer>();
		
		debugId = new ArrayList<Integer>();
		debugId.add(363);
		debugId.add(367);
		debugId.add(368);
		debugId.add(372);
		
	}
	
	public void naiveApproach(){
		System.out.println("Naive approach: (only Hash optimizing)");
		long sET = System.currentTimeMillis();
		//long startTime = System.currentTimeMillis();
		
		this.peoplePerPlace = new HashMap<String, HashSet<Integer>>();
		// One for all
		String[][] q = new String[this.queries.size()][3];
		eTime += System.currentTimeMillis() - sET;
		long sLT;
		for(String[] s : this.queries.toArray(q)){
			sLT = System.currentTimeMillis();
			this.peoplePerPlace.put(s[2], this.concernedPeople(s[2]));
			lTime += System.currentTimeMillis() - sLT;
			allConcernedPeople.addAll(this.peoplePerPlace.get(s[2]));
		}
		sLT = System.currentTimeMillis();
		nGraph = this.naiveGraph();
		lTime += System.currentTimeMillis() - sLT;
		sET = System.currentTimeMillis();
		int[][] minDist = this.shortestPaths(this.computeInitialMatrix(nGraph));	
		
		
		while(this.queries.isEmpty() == false){
			/*
			 * 1.Create a graph of concerned people
			 * 2.Min distance calculation
			 * 3.Enumerate all pairs of people within < h
			 * 4.For each pair, calculate tags in common and sort. Return k 
			 */
			String[] currentQ = this.queries.remove();
			String place = currentQ[2];
			int k = Integer.valueOf(currentQ[0]);
			int h = Integer.valueOf(currentQ[1]);
			ArrayList<Friends> concernedPairs = new ArrayList<Friends>();
			
			for(int i =0; i<minDist.length; i++){
				for(int j=i+1; j<minDist.length; j++){
					if(j<minDist.length){
						if(minDist[i][j] <= h && this.peoplePerPlace.get(place).contains(allPeople.get(i))  && this.peoplePerPlace.get(place).contains(allPeople.get(j)))
							concernedPairs.add(new Friends(nGraph.get(allPeople.get(i)), nGraph.get(allPeople.get(j))));
					}
				}
			}
			
			// First order by ID (needed to break ties)
			Collections.sort(concernedPairs, new FriendsComp(false));
			// Now sort by shared interests
			Collections.sort(concernedPairs, new FriendsComp(true));
			for(int i = 0; i < k && i < concernedPairs.size(); i++){
				System.out.print(concernedPairs.get(i).getIds() + " ");
			}
			System.out.print("% common interest counts ");
			for(int i = 0; i < k && i < concernedPairs.size(); i++){
				System.out.print(concernedPairs.get(i).getSharedInterests() + " ");
			}

			System.out.println();
		}
		eTime += System.currentTimeMillis() - sET;
		
		System.out.println("Loading time: " + this.lTime + "| Execution time: " + this.eTime + "| Total: " + (this.eTime + this.lTime));
		//System.out.println("Chrono read: " + (System.currentTimeMillis() - startTime) + "ms");
	}
	
	
	/*
	 * TO-DO fix search bug!!
	 */
	public void optimizedApproach(){
		System.out.println("Optimized approach: (Introducing Trees)");
		long sET = System.currentTimeMillis();
		
		
		this.peoplePerPlace = new HashMap<String, HashSet<Integer>>();
		// One for all
		String[][] q = new String[this.queries.size()][3];
		eTime += System.currentTimeMillis() - sET;
		long sLT;
		for(String[] s : this.queries.toArray(q)){
			sLT = System.currentTimeMillis();
			this.peoplePerPlace.put(s[2], this.concernedPeople(s[2]));
			lTime += System.currentTimeMillis() - sLT;
			sET = System.currentTimeMillis();
			allConcernedPeople.addAll(this.peoplePerPlace.get(s[2]));
			eTime += System.currentTimeMillis() - sET;
		}
		sLT = System.currentTimeMillis();
		oGraph =  this.optimizedGraph();
		
		lTime += System.currentTimeMillis() - sLT;
		while(this.queries.isEmpty() == false){
			/*
			 * 1.Create a graph of concerned people
			 * 2.Min distance calculation
			 * 3.Enumerate all pairs of people within < h
			 * 4.For each pair, calculate tags in common and sort. Return k 
			 */
			sET = System.currentTimeMillis();
			String[] currentQ = this.queries.remove();
			String place = currentQ[2];
			int k = Integer.valueOf(currentQ[0]);
			int h = Integer.valueOf(currentQ[1]);
			TreeMap<String, Friends> concernedPairs = new TreeMap<String, Friends>();
			
			int N = this.peoplePerPlace.get(place).size();
			Integer[] a =new Integer[N];
			a = this.peoplePerPlace.get(place).toArray(a);
			for(int i =0; i<N; i++){
				for(int j=i+1; j<N; j++){
					if(j<N){
						//System.out.println(oGraph.get("523"));
						//System.out.println(oGraph.get(a[i].toString()).getId() + " " + oGraph.get(a[j].toString()).getId());
						Friends fs = new Friends(oGraph.get(a[i].toString()), oGraph.get(a[j].toString()));
						if(fs.ok)
							concernedPairs.put(fs.getK(), fs);
					}
				}
			}
			String res = "";
			String comments = "% common interest counts ";
			int listed = 0;
			Friends candidate = null;
			while(listed < k){
				if (candidate == null){
					candidate = concernedPairs.firstEntry().getValue();
				}
				else{
					candidate = concernedPairs.higherEntry(candidate.getK()).getValue();
				}
				if (candidate.getDistance() <= h && candidate.getDistance() > 0){
					res += candidate.getIds() + " ";
					comments += candidate.getSharedInterests() + " ";
					listed++;
				}
			}
			System.out.println(res + comments);

		}
		eTime += System.currentTimeMillis() - sET;
		System.out.println("Loading time: " + this.lTime + "| Execution time: " + this.eTime + "| Total: " + (this.eTime + this.lTime));
	}	
	
	
	
	
	
	
	
	
	
	
	
	private HashSet<Integer> concernedPeople(String place){
		ArrayList<Integer> concernedPlaces = new ArrayList<Integer>();
		HashSet<Integer> concernedPeople = new HashSet<Integer>();
		ArrayList<Integer> concernedOrgs = new ArrayList<Integer>();
		int placeID = 0;
		
		//Find the ID of the place we're interested in
		try {
            BufferedReader file = new BufferedReader(new FileReader(this.dir + placeFile));
            String s;
            boolean go = true;
            // Ignore the first line
            file.readLine();
            while (( s = file.readLine() ) != null && go)  {
            	StringTokenizer st = new StringTokenizer(s,"|");
				Integer plID  = Integer.valueOf(st.nextToken());
				String pName = st.nextToken();
				if(pName.equals(place)){
					placeID = plID;
					go = false;
				}
            }
            file.close();
        } catch (IOException e) { e.printStackTrace(); }
		
		// Because this is the same for all queries, we only need to do it once.
		if(placeMap == null)
			placeMap = this.mapPlaces();
		
		concernedPlaces = placeMap.get(placeID).subPlaces();
		// Find the concerned organizations, i.e. the organizations that are in the concerned places
		
		try {
            BufferedReader file = new BufferedReader(new FileReader(this.dir + organisationLoc));
            String s;
            // Ignore the first line
            file.readLine();
            while (( s = file.readLine() ) != null)  {
            	StringTokenizer st = new StringTokenizer(s,"|");
				Integer orgId = Integer.valueOf(st.nextToken());
				Integer pId = Integer.valueOf(st.nextToken());
				// Making sure to check for duplicate people of course!
				if(concernedPlaces.contains(pId)){
					concernedOrgs.add(orgId);
				}
            }
            file.close();
        } catch (IOException e) { e.printStackTrace(); }
		
		//Find concerned people in 3 steps: ppl who work there, ppl who study there, ppl who are there
		try {
            BufferedReader file = new BufferedReader(new FileReader(this.dir + personPlaceLoc));
            String s;
            // Ignore the first line
            file.readLine();
            while (( s = file.readLine() ) != null)  {
            	StringTokenizer st = new StringTokenizer(s,"|");
				Integer personId = Integer.valueOf(st.nextToken());
				Integer placeId = Integer.valueOf(st.nextToken());
				if(concernedPlaces.contains(placeId)){
					concernedPeople.add(personId);
					//System.out.print("l");
				}
            }
            file.close();
        } catch (IOException e) { e.printStackTrace(); }
		try {
            BufferedReader file = new BufferedReader(new FileReader(this.dir + personStudy));
            String s;
            // Ignore the first line
            file.readLine();
            while (( s = file.readLine() ) != null)  {
            	StringTokenizer st = new StringTokenizer(s,"|");
				Integer personId = Integer.valueOf(st.nextToken());
				Integer orgId = Integer.valueOf(st.nextToken());
				if(concernedOrgs.contains(orgId) && !concernedPeople.contains(personId)){
					concernedPeople.add(personId);
					//System.out.print("s");
				}
            }
            file.close();
        } catch (IOException e) { e.printStackTrace(); }
		try {
            BufferedReader file = new BufferedReader(new FileReader(this.dir + personWork));
            String s;
            // Ignore the first line
            file.readLine();
            while (( s = file.readLine() ) != null)  {
            	StringTokenizer st = new StringTokenizer(s,"|");
				Integer personId = Integer.valueOf(st.nextToken());
				Integer orgId = Integer.valueOf(st.nextToken());
				if(concernedOrgs.contains(orgId) && !concernedPeople.contains(personId)){
					concernedPeople.add(personId);
					//System.out.print("w");
				}
            }
            file.close();
        } catch (IOException e) { e.printStackTrace(); }
		
		
		//System.out.println(concernedPeople.size() + " people concerned.");
		//System.out.println(concernedPeople.size() + " people concerned.");
		
		return concernedPeople;
	}
	
	
	private HashMap<Integer, SocialPerson> naiveGraph(){
		HashMap<Integer, SocialPerson> graph = new HashMap<Integer, SocialPerson>();
		
	
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
				if(!graph.containsKey(pId1)){
					graph.put(pId1, new SocialPerson(pId1));
					allPeople.add(pId1);
				}
				if(!graph.containsKey(pId2)){
					graph.put(pId2, new SocialPerson(pId2));
					allPeople.add(pId2);
				}
				graph.get(pId1).addFriend(graph.get(pId2));
				graph.get(pId2).addFriend(graph.get(pId1));
            }
            file.close();
        } catch (IOException e) { e.printStackTrace(); }
		
		//Populate interests
		try {
            BufferedReader file = new BufferedReader(new FileReader(this.dir + personInterests));
            String s;
            // Ignore the first line
            file.readLine();
            while (( s = file.readLine() ) != null)  {
            	StringTokenizer st = new StringTokenizer(s,"|");
				Integer pId = Integer.valueOf(st.nextToken());
				Integer iId = Integer.valueOf(st.nextToken());
				if(this.allConcernedPeople.contains(pId) && graph.containsKey(pId)){
					graph.get(pId).addInterest(iId);
				}
            }
            file.close();
        } catch (IOException e) { e.printStackTrace(); }
		
		return graph;
	}
	
	private TreeMap<String, SocialPerson> optimizedGraph(){
		TreeMap<String, SocialPerson> graph = new TreeMap<String, SocialPerson>();
		
		//Populate friendship arrays
		try {
            BufferedReader file = new BufferedReader(new FileReader(this.dir + friendshipFile));
            String s;
            // Ignore the first line
            file.readLine();
            while (( s = file.readLine() ) != null)  {
            	StringTokenizer st = new StringTokenizer(s,"|");
				String pId1 = "" + Integer.valueOf(st.nextToken());
				String pId2 = "" + Integer.valueOf(st.nextToken());
				if(!graph.containsKey(pId1)){
					graph.put(pId1, new SocialPerson(Integer.valueOf(pId1), true));
					allPeople.add(Integer.valueOf(pId1));
					//System.out.println("Added " + pId1);
				}
				if(!graph.containsKey(pId2)){
					graph.put(pId2, new SocialPerson(Integer.valueOf(pId2), true));
					allPeople.add(Integer.valueOf(pId2));
					//System.out.println("Added " + pId2);
				}
				graph.get(pId1).addFriend(graph.get(pId2));
				graph.get(pId2).addFriend(graph.get(pId1));
            }
            file.close();
        } catch (IOException e) { e.printStackTrace(); }
		
		//Populate interests
		try {
            BufferedReader file = new BufferedReader(new FileReader(this.dir + personInterests));
            String s;
            // Ignore the first line
            file.readLine();
            while (( s = file.readLine() ) != null)  {
            	StringTokenizer st = new StringTokenizer(s,"|");
				Integer pId = Integer.valueOf(st.nextToken());
				Integer iId = Integer.valueOf(st.nextToken());
				if(this.allConcernedPeople.contains(pId) && graph.containsKey(pId.toString())){
					graph.get(pId.toString()).addInterest(iId);
				}
            }
            file.close();
        } catch (IOException e) { e.printStackTrace(); }
		
		
		
		
		return graph;
	}
	
	private HashMap<Integer, Place> mapPlaces(){
		HashMap<Integer, Place> res = new HashMap<Integer, Place>();
		//Find concerned places
		try {
            BufferedReader file = new BufferedReader(new FileReader(this.dir + placePart));
            String s;
            // Ignore the first line
            file.readLine();
            while (( s = file.readLine() ) != null)  {
            	StringTokenizer st = new StringTokenizer(s,"|");
				Integer pId1 = Integer.valueOf(st.nextToken());
				Integer pId2 = Integer.valueOf(st.nextToken());
				// Add the places we are just finding
				if(!res.containsKey(pId1)){
					res.put(pId1, new Place(pId1));
				}
				if(!res.containsKey(pId2)){
					res.put(pId2, new Place(pId2));
				}
				res.get(pId2).addSub(res.get(pId1));
            }
            file.close();
        } catch (IOException e) { e.printStackTrace(); }
		return res;
	}	
	
	/*
	 * Using edges, compute the initial adjacency matrix of the graph
	 * (Possible optimization: because it's undirected, we could do half the work)
	 */
	private int[][] computeInitialMatrix(HashMap<Integer, SocialPerson> graph){
		int n = graph.size();
		int[][] initialMatrix = new int[n][n];
		for(int i = 0; i < n; i++){
			for(int j = i; j < n; j++){
				if(graph.get(allPeople.get(i)).knows(graph.get(allPeople.get(j)))){
					initialMatrix[i][j] = 1;
					initialMatrix[j][i] = 1;
				}
				else if(i == j){
					initialMatrix[i][j] = 0;
				}
				else{
					// This is longer than any shortest path can be
					initialMatrix[i][j] = 2 * n;
					initialMatrix[j][i] = 2 * n;
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

	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	public void optimization1(){
		System.out.println("First optimization:");
		long startTime = System.currentTimeMillis();
		while(this.queries.isEmpty() == false){
			/*
			 * 1.Create a graph of concerned people
			 * 2.Min distance calculation
			 * 3.Sort all the people by T = total number of tags.
			 * 4.Iterate down while max_matches < current person's T
			 * 5. For each person below, if matching_tags > max_matches and minD < h, update max_matches
			 * (Potentially much faster as top matches are bound to be within people with most tags
			 * and it's n^2 in the worst case)
			 */
			System.out.println();
		}
		System.out.println("Chrono read: " + (System.currentTimeMillis() - startTime) + "ms");
	}
	
	
	
	
	
	
}
