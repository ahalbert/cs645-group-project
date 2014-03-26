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
import java.util.Queue;
import java.util.ArrayDeque;

public class query2 {
	public TreeMap<String, Person> people = new TreeMap<String, Person>();
	public TreeMap<String, Integer> interestSize;
	public Calendar lastCalendar = Calendar.getInstance();
	private HashMap<String, Integer> map;
	
	public query2() {
        	
	}

	public void queries() {
	    try {
            BufferedReader file = new BufferedReader(new FileReader("./data/data.txt"));
            String s;
            Calendar c = Calendar.getInstance();
            while (( s = file.readLine() ) != null)  {
                //parse strings
                s = s.substring(7, s.length()-1);
                s = s.replaceAll("\\s","");
                String[] values = s.split(",");
                int k = new Integer(values[0]);
                String[] dates = values[1].split("-");
                c.set(new Integer(dates[0]), new Integer(dates[1]), new Integer( dates[2] ));

                eval(k,c); 
                people = new TreeMap<String, Person>(); //Used to fix a bug where people would be loaded multiple times.
            }

            file.close();
        } catch (IOException e) { e.printStackTrace(); }
	}

	public void sortFile() {
        TreeMap<String, String> bdates = new TreeMap<String, String>();
        try {
            BufferedReader file = new BufferedReader(new FileReader("data/person.csv"));
            String s;
            file.readLine();
	        while (( s = file.readLine() ) != null)  {
                String[] values = s.split("\\|");
                String birthday = values[4];
                bdates.put(birthday,s);
	        }
            file.close();
            BufferedWriter wfile = new BufferedWriter(new FileWriter("data/person.csv"));
            Iterator it = bdates.descendingMap().entrySet().iterator();
            while(it.hasNext()) {
                Map.Entry<String, String> kvpair = (Map.Entry)it.next();
                wfile.write(kvpair.getValue()+"\n");
            }
            wfile.close();
        } catch(Exception e) {}
	}
	
	public ArrayList<String> eval(int k, Calendar d) {
	    map = new HashMap<String,Integer>();
	    ValueComparator c = new ValueComparator(map);
        interestSize = new TreeMap<String, Integer>(c);
	    loadPeople(d);
        
	    HashMap<String, Integer> tagmap = new HashMap<String,Integer>();
	    ValueComparator c2 = new ValueComparator(tagmap);
        TreeMap<String, Integer> tags = new TreeMap<String, Integer>(c2);

        Iterator it = interestSize.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, Integer> kvpair = (Map.Entry)it.next();
            int tagsize = searchInterest(kvpair.getKey());
            tagmap.put(kvpair.getKey(), tagsize );
        }
        tags.putAll(tagmap);
        System.out.println(tags);
        it = tags.entrySet().iterator();
        
        ArrayList<String> ret = new ArrayList<String>();
        int sz = -1;
        for (int i = 0; i < k; i++)  {
            Map.Entry<String,Integer> tag = (Map.Entry<String,Integer>)it.next();
            sz = tag.getValue();
            System.out.println(tag.getKey() + " "+ lookupTagDef(tag.getKey()));
            String s = lookupTagDef(tag.getKey());
            ret.add(s);
        }

        Map.Entry<String,Integer> tag = (Map.Entry<String,Integer>)it.next();
        while(tag.getValue() == sz) {
            System.out.println(tag.getKey() + " "+ lookupTagDef(tag.getKey()));
            String s = lookupTagDef(tag.getKey());
            ret.add(s);
            tag = (Map.Entry<String,Integer>)it.next();
        }
        System.out.println(ret);
	    return ret;
	} 

    private String lookupTagDef(String tag) {
        try {
            BufferedReader file = new BufferedReader(new FileReader("./data/tag.csv"));
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

    /*
     * Gets a starting point for each tree.
     * Optimization: We essentially double-calculate the size of a tree. We should only do this once.
     * */ 
	private ArrayList<Person> getTreesInForest(String interest) {
	    HashMap<String, Person> peopleWithInterest = new HashMap<String, Person>();
	    Iterator it = people.entrySet().iterator();
	    while (it.hasNext()) {
            Map.Entry<String, Person> kvpair = (Map.Entry)it.next();
            if (kvpair.getValue().interests.contains(interest)) {
                peopleWithInterest.put(kvpair.getKey(), kvpair.getValue());
            }
	    }

	    it = peopleWithInterest.entrySet().iterator();
	    ArrayList<Person> visited = new ArrayList<Person>();
	    ArrayList<Person> trees = new ArrayList<Person>();
	    while(it.hasNext()) {
            Map.Entry<String, Person> kvpair = (Map.Entry)it.next();
            if(!visited.contains(kvpair.getValue())) {
                visited.add(kvpair.getValue());
                trees.add(kvpair.getValue());
                ArrayList<Person> v = getConnectedNodes(kvpair.getValue(), interest);
                Iterator ita = v.iterator();
                while(ita.hasNext()) {
                    visited.add((Person)ita.next());
                }
            }
        }
	    return trees;
	}

	/*
	 * Gets all person_knows_person connections where persons share an interest.
	 * */

	private ArrayList<Person> getConnectedNodes(Person p, String interest) {
	    ArrayDeque<Person> nextQueue = new ArrayDeque <Person>();
	    nextQueue.push(p);
	    ArrayList<Person> found = new ArrayList<Person>();
	    found.add(p);
	    while (!nextQueue.isEmpty()) {
            Person person = nextQueue.pop();
            Iterator it = person.friends.iterator();
            while (it.hasNext()) {
                Person f = (Person)it.next();
                if (! found.contains(f) && isStringInArrayList(f.interests, interest)) {
                    found.add(f);
                    nextQueue.add(f);
                }
            }
        }
	    return found;
	}

    //TODO: Remove this.
	private boolean isStringInArrayList(ArrayList<String> list, String s) {
	    Iterator it = list.iterator();
	    while(it.hasNext()) {
	        if (s.compareTo((String)it.next()) == 0) 
	            return true;
	    }
        return false;	
	}


    /*
     * Gets largest tree for interest i
     * */ 
	private int searchInterest(String interest) {
	    ArrayList<Person> treeStart = getTreesInForest(interest);
	    int size = 0;
	    Iterator it = treeStart.iterator();
	    while (it.hasNext()) {
	        int graphSize = getConnectedNodes((Person)it.next(), interest).size();
	        if ( graphSize > size)  {
	            size = graphSize;
	        }
	    }
        return size;	
	}

    /* Loads people into the data structure, gathers their interests, and 
     * The interestSize hash table is used to sort the interests by the largest size.
     * This is is used to predict which tags will be the most frutiful.
     *
     * */
    
	private void loadPeople(Calendar d) {
	    //Assumes file is sorted.
	    try {
            BufferedReader file = new BufferedReader(new FileReader("data/person.csv"));
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
            BufferedReader file = new BufferedReader(new FileReader("./data/person_hasInterest_tag.csv"));
            String s;
            file.readLine();

	        while (( s = file.readLine() ) != null)  {
	            String[] values = s.split("\\|");
	            String id = values[0];
                String interest = values[1];
                Person p;

                if ((p = people.get(id)) != null)
                    p.addInterest(interest);

                Integer  ientry;
                
                    if((ientry = map.get(interest)) == null)
                        map.put(interest,0);
                    else
                        if (p != null) {
                            map.put(interest, ientry + 1);
                    }
	        }
            file.close();
            interestSize.putAll(map);
            System.out.println(interestSize);
	    } catch(IOException e) { e.printStackTrace(); }

	    try {
            BufferedReader file = new BufferedReader(new FileReader("./data/person_knows_person.csv"));
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
}

