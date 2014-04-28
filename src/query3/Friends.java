package query3;

public class Friends {

	private SocialPerson p1, p2;
	private int sharedInterests;
	
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
	
	public int getFirstId(){
		return p1.getId();
	}
	
}
