package query3;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import query4.Person;

public class SocialPerson extends Person {
	
	private ArrayList<SocialPerson> friends;
	private ArrayList<Integer> interests;
	public HashMap<SocialPerson, Integer> minDist;
	private int ctr;
	
	public SocialPerson(int id){
		super(id);
		this.friends = new ArrayList<SocialPerson>();
		this.interests = new ArrayList<Integer>();
	}
	
	public SocialPerson(int id, boolean opt){
		this(id);
		if(opt){
			minDist = new HashMap<SocialPerson, Integer>();
			minDist.put(this, 0);
			ctr = 0;
		}
	}


	public void addFriend(SocialPerson person) {
		this.friends.add(person);
	}
	
	public boolean knows(SocialPerson person){
		return friends.contains(person);
	}
	

	public void addInterest(Integer iID) {
		this.interests.add(iID);
	}
	
	public ArrayList<Integer> getInterests() {
		return interests;
	}


	public boolean likes(int iID){
		return interests.contains(iID);
	}
	
	public SocialPerson[] BFSstep(){
		ArrayList<SocialPerson> newest = new ArrayList<SocialPerson>();
		
		if(ctr == 0){
			for (SocialPerson p : this.friends){
				newest.add(p);
			}
			for(SocialPerson c : newest){
				this.minDist.put(c, ctr+1);
				if(this.getId() == 280){
					System.out.println(c.getId() + " " + (ctr+1));
				}
			}
		}
		else{
			for(Map.Entry<SocialPerson, Integer> p : this.minDist.entrySet()){
				if(p.getValue() == ctr ){
					for(SocialPerson c : p.getKey().getFriends()){
						if(!this.minDist.containsKey(c)){
							newest.add(c);
						}
					}	
				}
			}
			for(SocialPerson c : newest){

				if(this.getId() == 280){
					//System.out.println(c.getId() + " " + (ctr+1));
				}
				this.minDist.put(c, ctr+1);
			}
		}
		ctr++;
		
		
		
		
		return newest.toArray(new SocialPerson[newest.size()]);
		
	}

	public ArrayList<SocialPerson> getFriends() {
		return friends;
	}
	
	

}
