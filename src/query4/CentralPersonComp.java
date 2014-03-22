package query4;

import java.util.Comparator;

public class CentralPersonComp implements Comparator<CentralPerson>{

	boolean centrality;
	
	public CentralPersonComp(boolean c){
		centrality = c;
	}
	
	/*
	 * Compare based on id or centrality depending on the boolean
	 */
	@Override
	public int compare(CentralPerson first, CentralPerson other) {
		if(this.centrality){
			if(first.getCentrality() > other.getCentrality())
				return -1;
			else if(first.getCentrality() < other.getCentrality())
				return 1;
			else
				return 0;
		}
		else{
			if(first.getId() > other.getId())
				return 1;
			else if(first.getId() < other.getId())
				return -1;
			else
				return 0;
		}
	}
	
}
