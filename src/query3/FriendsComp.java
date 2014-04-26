package query3;

import java.util.Comparator;

public class FriendsComp implements Comparator<Friends>{

	boolean interests;
	
	public FriendsComp(boolean i){
		interests = i;
	}
	
	/*
	 * Compare based on id or interests depending on the boolean
	 */
	@Override
	public int compare(Friends first, Friends other) {
		if(this.interests){
			if(first.getSharedInterests() > other.getSharedInterests())
				return -1;
			else if(first.getSharedInterests() < other.getSharedInterests())
				return 1;
			else
				return 0;
		}
		else{
			if(first.getFirstId() > other.getFirstId())
				return 1;
			else if(first.getFirstId() < other.getFirstId())
				return -1;
			else
				return 0;
		}
	}
	
}
