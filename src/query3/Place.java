package query3;

import java.util.ArrayList;

public class Place {

	private int id;
	private String name;
	private ArrayList<Place> contains;
	
	public Place(int i, String n){
		this.id = i;
		this.name = n;
		this.contains = new ArrayList<Place>();
	}
	
	public Place(int i){
		this.id = i;
		this.contains = new ArrayList<Place>();
	}
	
	public boolean contains(int pID){
		return false;
	}
	
	public int getID(){
		return this.id;
	}
	
	public ArrayList<Integer> subPlaces(){
		ArrayList<Integer> sp = new ArrayList<Integer>();
		sp.add(this.id);
		for(Place p : this.contains){
			sp.addAll(p.subPlaces());
		}
		return sp;
	}
	
	public void addSub(Place sP){
		contains.add(sP);
	}
	
}
