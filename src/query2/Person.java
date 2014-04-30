package query2;
import java.util.ArrayList;
import java.util.Iterator;

public class Person {
	public String id;
	public ArrayList<String> interests = new ArrayList<String>();
	public ArrayList<Person> friends = new ArrayList<Person>();

	public Person(String i) {
	}
	
	public void addInterest(String interest) {
        interests.add(interest);
	}

	public void addFriend(Person friend) {
        friends.add(friend);
	}
	public boolean hasInterest(String i) {
	    Iterator<String> it = interests.iterator();
        while (it.hasNext()) {
            String s = it.next();
            if(s.compareTo(i) == 0)
                return true;
        }
        return false;
	}
}
