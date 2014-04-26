package query3;

import java.util.ArrayList;

import query4.Person;

public class SocialPerson extends Person {
	
	private ArrayList<Integer> friends;
	private ArrayList<Integer> interests;
	
	public SocialPerson(int id){
		super(id);
		this.friends = new ArrayList<Integer>();
		this.interests = new ArrayList<Integer>();
	}


	public void addFriend(Integer pID) {
		this.friends.add(pID);
	}
	
	public boolean knows(int pID){
		return friends.contains(pID);
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
	
	

}
