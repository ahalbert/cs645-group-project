package query3;

import java.util.HashMap;

public class Friends {

	private SocialPerson p1, p2;
	private int sharedInterests;
	private int distance;
	boolean c;
	public boolean ok;
	
	public Friends(SocialPerson f1, SocialPerson f2){
		ok = true;
		if(f2 == null || f1 == null){
			ok = false;
			return;
		}
		
		if(f1.getId() < f2.getId()){
			p1 = f1;
			p2 = f2;
		}
		else{
			p1 = f2;
			p2 = f1;
		}
		sharedInterests = 0;
		for(int i : p1.getInterests()){
			if(p2.likes(i)){
				sharedInterests++;
			}
		}
		
		distance = 0;
		//Debugging
		boolean a = (p1.getId() == 361 && p2.getId() == 812);
		boolean b = (p1.getId() == 280 && p2.getId() == 812);
		boolean d = (p1.getId() == 361 && p2.getId() == 812);
		c = (p1.getId() == 280 && p2.getId() == 812);

		
	}
	
	public Friends(SocialPerson f1, SocialPerson f2, int d){
		this(f1, f2);
		this.distance = d;
	}

	public int getSharedInterests() {
		return sharedInterests;
	}
	
	public int getP1(){
		return p1.getId();
	}
	
	public int getP2(){
		return p2.getId();
	}
	
	public String getIds(){
		return p1.getId() + "|" + p2.getId();
	}

	public int getDistance() {
		if(distance > 0){
			return distance;
		}
		distance = this.minDist();
		return distance;
	}
	
	public String getK(){
		return String.format("%04d", 9999-sharedInterests) + String.format("%06d", p1.getId()) + String.format("%06d", p2.getId());
	}
	
	private int minDist(){
		int m = -1;
		SocialPerson[] p1leaves, p2leaves;
		while(m == -1){
			p1leaves =  this.p1.BFSstep();
			p2leaves =  this.p2.BFSstep();
			for (SocialPerson id1 : p1leaves){
				if(c)
					System.out.println("[1]"+id1.getId());
				if(this.p2.minDist.containsKey(id1)){
					if(c)
						System.out.println("[1]"+this.p2.minDist.get(id1) + this.p1.minDist.get(id1));
					return this.p2.minDist.get(id1) + this.p1.minDist.get(id1);
				}
			}
			//System.out.print("|");
			for (SocialPerson id2 : p2leaves){
				if(c)
					System.out.println("[2]"+id2.getId());
				if(this.p1.minDist.containsKey(id2)){
					return this.p1.minDist.get(id2) + this.p2.minDist.get(id2);
				}
			}
			//System.out.println();
		}
		return m;
	}
	
}
