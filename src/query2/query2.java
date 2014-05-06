package query2;
	    
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.TreeMap;
import java.util.HashMap;
import java.util.Map;
import java.util.Comparator;
import java.util.Iterator;
import java.util.ArrayDeque;

public class query2 {
	public TreeMap<String, Person> people = new TreeMap<String, Person>();
	public TreeMap<String, Integer> interestSize;
	public Calendar lastCalendar = Calendar.getInstance();
	private HashMap<String, Integer> map;
    private HashMap<String, Integer> hashindex = new HashMap<String, Integer>();
    private TreeMap<String, Integer> Btreeindex = new TreeMap<String, Integer>();
    private String indexType;
	
	public query2(String mode, String fileloc) {
	    indexType = mode;
        if(mode.compareTo("hash") == 0)
           hashIndex(fileloc);
        if(mode.compareTo("tree") == 0)
            treeIndex(fileloc);
	}

	public void sortFile(String fileloc) {
        TreeMap<String, ArrayList<String>> bdates = new TreeMap<String, ArrayList<String>>();

        try {
            BufferedReader file = new BufferedReader(new FileReader(fileloc+"person.csv"));
            String s;
	        while (( s = file.readLine() ) != null)  {
	            if (s.compareTo("id|firstName|lastName|gender|birthday|creationDate|locationIP|browserUsed") != 0) {
                    String[] values = s.split("\\|");
                    String birthday = values[4];
                    ArrayList<String> al = bdates.get(birthday);
                    if(al == null)
                        bdates.put(birthday, new ArrayList<String>());
                    al = bdates.get(birthday);
                    al.add(s);
                }
	        }
            file.close();
            BufferedWriter wfile = new BufferedWriter(new FileWriter(fileloc+"person.csv"));
            Iterator it = bdates.descendingMap().entrySet().iterator();
            while(it.hasNext()) {
                Map.Entry<String, ArrayList<String>> kvpair = (Map.Entry<String, ArrayList<String>>)it.next();
                Iterator ita = kvpair.getValue().iterator();
                while (ita.hasNext())
                    wfile.write(ita.next()+"\n");
            }
            wfile.close();
        } catch(Exception e) {}
	}
	
	public ArrayList<String> eval(int k, Calendar d, String fileloc) {
	    map = new HashMap<String,Integer>();
	    ValueComparator c = new ValueComparator(map);
        interestSize = new TreeMap<String, Integer>(c);
	    loadPeople(d, fileloc);
        
        TreeMap<Integer, ArrayList<String>> tags = new TreeMap<Integer, ArrayList<String>>();

        Iterator it = interestSize.entrySet().iterator();
        int[] khighest = new int[k];
        for (int i = 0; i < k; i++) 
            khighest[i] = 0;
        while (it.hasNext()) {
            Map.Entry<String, Integer> kvpair = (Map.Entry)it.next();
            if (kvpair.getValue() < khighest[k-1])
                break;
            int tagsize = getLargestFamily(kvpair.getKey());
            insertIntoKhighestArray(tagsize, khighest);
            if (tags.get(tagsize) == null)
                tags.put(tagsize, new ArrayList<String>());
            ArrayList<String> al = tags.get(tagsize);
            insertIntoArrayList(al, lookupTagDef(kvpair.getKey(), fileloc));
        }

        ArrayList<String> ret = new ArrayList<String>();
        it = tags.descendingMap().entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<Integer, ArrayList<String>> a = (Map.Entry<Integer, ArrayList<String>>)it.next(); 
            ArrayList<String> al = a.getValue();
            if(k == 0) {
                break;
            }
                if(k > 0) {
                    Iterator ita = al.iterator();
                    while (ita.hasNext() && k > 0) {
                        System.out.print(ita.next() + " ");
                        k--; 
                    }
                }
        }
	    return ret;
	} 

	private void insertIntoKhighestArray(int a, int[] array) {
        for (int i = 0; i < array.length; i++)  {
            if(array[i] == 0) {
                array[i] = a;
                return;
            }
        }
        for (int i = 0; i < array.length; i++)  {
            if (a > array[i]){
                int temp = a;
                for (int j = i; j < array.length; j++) {
                        int temp2 = array[i];
                        array[i] = temp;
                        temp = temp2;
                }
            }
        }
	}

    private void insertIntoArrayList(ArrayList<String> a, String item) {
        Iterator<String> it = a.iterator();
        int loc = -1;
        boolean added = false;
        while (it.hasNext()) {
            String s = it.next(); 
            loc++;
            if(precedence(item, s)) {
                a.add(loc, item);
                added = true;
                break;
            }
        }
        if (!added) 
            a.add(item); 
    }
    private boolean precedence(String a, String b) {
        int minLength = Math.min(a.length(), b.length());
        for (int i = 0; i < minLength; i++ )  {
            if (Character.isLetter(a.charAt(i)) && !Character.isLetter(b.charAt(i)))
                return true;
            if (!Character.isLetter(a.charAt(i)) && Character.isLetter(b.charAt(i)))
                return false;
            else if (Character.toLowerCase(a.charAt(i)) < Character.toLowerCase(b.charAt(i)))
                return true;
            else if (Character.toLowerCase(a.charAt(i)) > Character.toLowerCase(b.charAt(i)))
                return false;
        }
        if (a.length() > b.length())
            return true;
        return false;
    }

    private int getLargestFamily(String interest) {
        ArrayList<Person> visited = new ArrayList<Person>();
        int familySize = 0;
        int t;
	    Iterator<Map.Entry<String,Person>> it = people.entrySet().iterator();
        Person p = it.next().getValue();
	    while (it.hasNext()) {
	        if (!visited.contains(p) && p.hasInterest(interest)) {
                t = BFS(p, interest, visited);
                if (t > familySize) {
                    familySize = t;
                }
	        }
            p = it.next().getValue(); 
	    }
        return familySize;
    }

    private int BFS(Person p, String interest, ArrayList<Person> visited) {
        int size = 1;
	    ArrayDeque<Person> nextQueue = new ArrayDeque<Person>();
	    ArrayList<Person> friends = getFriendsWithInterest(p, interest);
        Iterator<Person> it = friends.iterator();
        Person temp;
	    while(it.hasNext()) {
            temp = it.next();
	        if (!visited.contains(temp)) {
                nextQueue.push(temp);
                visited.add(temp);
                size++;
	        }
	    }
	    while(!nextQueue.isEmpty()) {
            temp = nextQueue.pop();
            friends = getFriendsWithInterest(temp, interest);
            it = friends.iterator();
            while(it.hasNext()) {
                temp = it.next();
                if (!visited.contains(temp)) {
                    nextQueue.push(temp);
                    visited.add(temp);
                    size++;
                }
            }
	    }
	    return size;
    }

    private ArrayList<Person> getFriendsWithInterest(Person p, String interest) {
        ArrayList<Person> friends = new ArrayList<Person>();
        Iterator<Person> it = p.friends.iterator();
        Person friend;
        while (it.hasNext()) {
           friend = it.next(); 
           if (friend.hasInterest(interest)) {
                friends.add(friend); 
           }
        }
        return friends;
    }

    private String lookupTagDef(String tag, String fileloc) {
        try {
            BufferedReader file = new BufferedReader(new FileReader(fileloc+"tag.csv"));
            file.readLine();
            String s;
            while (( s = file.readLine() ) != null)  {
                String[] values = s.split("\\|");
                if (values[0].compareTo(tag) == 0) {
                    file.close();
                    return values[1];
                }
            }
            file.close();
        } catch (IOException e) { e.printStackTrace(); }
        return "";
    }


    /* Loads people into the data structure, gathers their interests, and 
     * The interestSize hash table is used to sort the interests by the largest size.
     * This is is used to predict which tags will be the most frutiful.
     *
     * */
    
	private void loadPeople(Calendar d, String fileloc) {
	    //Assumes file is sorted.
        people = new TreeMap<String, Person>();
	    try {
            BufferedReader file = new BufferedReader(new FileReader(fileloc+"person.csv"));
            String s;
            String[] values;

	        while (( s = file.readLine() ) != null)  {
                values = s.split("\\|");
                String birthday = values[4];
                String id = values[0]; 
                values = birthday.split("-");
                Calendar birthdate = Calendar.getInstance();
                birthdate.set(Integer.parseInt(values[0]), Integer.parseInt( values[1]), Integer.parseInt(values[2]));

                if (birthdate.before(d)) {
                    break;
                }
                people.put(id, new Person(id));

	        }
            file.close();
	    } catch(IOException e) { e.printStackTrace(); }

	    try {
            BufferedReader file = new BufferedReader(new FileReader(fileloc+"person_hasInterest_tag.csv"));
            String s;
            file.readLine();

	        while (( s = file.readLine() ) != null)  {
	            String[] values = s.split("\\|");
	            String id = values[0];
                String interest = values[1];
                Person p;

                if ((p = people.get(id)) != null) {
                    p.addInterest(interest);

                    Integer  ientry;
                
                    if((ientry = map.get(interest)) == null)
                        map.put(interest,1);
                    else {
                        map.remove(interest);
                        map.put(interest, ientry + 1);
                    }
                }
	        }
            file.close();
            interestSize.putAll(map);
	    } catch(IOException e) { e.printStackTrace(); }

	    try {
            BufferedReader file = new BufferedReader(new FileReader(fileloc+"person_knows_person.csv"));
            String s;
            file.readLine();
            while((s = file.readLine()) != null) {
	            String[] values = s.split("\\|");
                Person source = people.get(values[0]);
                Person target = people.get(values[1]);
                if (source != null && target != null)
                   source.addFriend(target);
            }
	    } catch(IOException e) { e.printStackTrace(); }
	} 

    private void treeIndex(String fileloc) {
        String s;
        try {
            BufferedReader file = new BufferedReader(new FileReader(fileloc+"person_hasInterest_tag.csv"));
            file.readLine();
            int lineCount = 1;
            String oldPerson = "";
            while((s = file.readLine()) != null) {
                String[] values = s.split("\\|");
                String person = values[0];
                if(person.compareTo(oldPerson) != 0) {
                    oldPerson = person; 
                    Btreeindex.put(person,lineCount);
                }
                lineCount++;
            }
            file.close();
        } catch(IOException e) {
        }
    }

    private void  hashIndex(String fileloc) {
        String s;
        try {
            BufferedReader file = new BufferedReader(new FileReader(fileloc+"person_hasInterest_tag.csv"));
            file.readLine();
            int lineCount = 1;
            String oldPerson = "";
            while((s = file.readLine()) != null) {
                String[] values = s.split("\\|");
                String person = values[0];
                if(person.compareTo(oldPerson) != 0) {
                    oldPerson = person; 
                    hashindex.put(person,lineCount);
                }
                lineCount++;
            }
            file.close();
        } catch(IOException e) {
        }
    }
    //Used to order tree maps by value 
    private class ValueComparator implements Comparator<String> {

        Map<String, Integer> base;
        public ValueComparator(Map<String, Integer> base) {
            this.base = base;
        }

        // Note: this comparator imposes orderings that are inconsistent with equals.    
        public int compare(String a, String b) {
            if (base.get(a) >= base.get(b)) {
                return -1;
            } else {
                return 1;
            } // returning 0 would merge keys
        }
    }

    private class SSValueComparator implements Comparator<String> {

        Map<String, String> base;
        public SSValueComparator(Map<String, String> base) {
            this.base = base;
        }

        public int compare(String a, String b) {
            return a.compareTo(b);
        } // returning 0 would merge keys
    }
}

