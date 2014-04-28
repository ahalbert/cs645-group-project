package query4;

import java.util.ArrayList;

public class CentralPerson extends Person{
	

	private float centrality;
	private ArrayList<Integer> friends;
	
	public CentralPerson(int id){
		super(id);
		this.friends = new ArrayList<Integer>();
	}


	public float getCentrality() {
		return centrality;
	}

	public void addFriend(Integer pID) {
		this.friends.add(pID);
	}
	
	public boolean knows(int pID){
		return friends.contains(pID);
	}
	public void setCentrality(float centrality) {
		this.centrality = centrality;
	}
	

}
