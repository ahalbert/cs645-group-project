package query3;

public class Friends {

	private SocialPerson p1, p2;
	private int sharedInterests;
	private int distance;
	
	public Friends(SocialPerson f1, SocialPerson f2){
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
		
		
		//Debugging
		boolean a = (p1.getId() == 361 && p2.getId() == 812);
		boolean b = (p1.getId() == 280 && p2.getId() == 812);
		boolean c = (p1.getId() == 363 && p2.getId() == 367);
		boolean d = (p1.getId() == 361 && p2.getId() == 812);
		
		
		
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
		return distance;
	}
	
	public String getK(){
		return String.format("%04d", 9999-sharedInterests) + String.format("%06d", p1.getId()) + String.format("%06d", p2.getId());
	}
	
}
