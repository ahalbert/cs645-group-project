package query4;

import java.util.ArrayList;

public class CentralPerson{
	
	private int id;
	private float centrality;
	private ArrayList<Integer> friends;
	
	public CentralPerson(int id){
		this.id = id;
		this.friends = new ArrayList<Integer>();
	}

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public float getCentrality() {
		return centrality;
	}

	public void addFriend(Integer pID) {
		this.friends.add(pID);
	}
	
	public boolean knows(int pID){
		if(this.friends.size() > 0){
			for(Integer id : this.friends){
				if(id == pID){
					return true;
				}
			}
		}
		return false;
	}
	public void setCentrality(float centrality) {
		this.centrality = centrality;
	}
	

}
