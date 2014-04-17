package query3;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.Queue;

import query4.CentralPerson;
import query4.CentralPersonComp;

public class Socializer {

	static String personInterests = "person_hasInterest_tag.csv";
	static String personPlaceLoc = "person_isLocatedIn_place.csv";
	static String personPlacePart = "place_isPartOf_place.csv";
	static String personStudy = "person_studyAt_organisation.csv";
	static String personWork = "person_workAt_organisation.csv";
	static String organisationLoc = "organisation_isLocatedIn_place.csv";
	static String placePart = "place_isPartOf_place.csv";
	static String placeFile = "place.csv";
	static String friendshipFile = "person_knows_person.csv";
	
	String dir;
	Queue<String []> queries;
	
	public Socializer(String dataDir, String queryF){		
		queries = new LinkedList<String[]>();
		this.dir = dataDir;
		try {
            BufferedReader file = new BufferedReader(new FileReader(queryF));
            String s;
            while (( s = file.readLine() ) != null)  {
                s = s.substring(7, s.length()-1);
                s = s.replaceAll("\\s","");
                String[] values = s.split(",");
                this.queries.add(values);
            }
            file.close();
        } catch (IOException e) { e.printStackTrace(); }
		System.out.println(this.queries.size() + " queries3 loaded.");
	}
	
	public void naiveApproach(){
		System.out.println("Naive approach:");
		long startTime = System.currentTimeMillis();
		while(this.queries.isEmpty() == false){
			/*
			 * 1.Create a graph of concerned people
			 * 2.Min distance calculation
			 * 3.Enumerate all pairs of people within < h
			 * 4.For each pair, calculate tags in common and sort. Return k 
			 */
			System.out.println();
		}
		System.out.println("Chrono read: " + (System.currentTimeMillis() - startTime) + "ms");
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
