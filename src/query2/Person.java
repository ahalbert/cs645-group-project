package query2;
import java.util.ArrayList;

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
}
