
import query2.query2;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Calendar;

public class Dispatcher {
    public static void main (String[] args) {
        String fileloc = args[0];
        String queryloc = args[1];
        query2 q2 = new query2("",fileloc);
        q2.sortFile(fileloc);
        Calendar c = Calendar.getInstance();
        try {
            BufferedReader file = new BufferedReader(new FileReader(queryloc));
            String s;
            while (( s = file.readLine() ) != null) {
                String type =  s.substring(0, 6);
                if (type.compareTo("query1") == 0) {}
                if (type.compareTo("query2") == 0) {
                    //Split string into parameters
                    System.out.println("\n"+s);
                    int start = s.indexOf('(');
                    int end = s.indexOf(')');
                    s= s.substring(start+1,end);//+1 to get red of starting (
                    String[] values = s.split(", ");
                    int k = new Integer(values[0]);
                    String[] dates = values[1].split("-");
                    c.set(new Integer(dates[0]), new Integer(dates[1]), new Integer( dates[2] ));

                    q2.eval(k,c, fileloc); 
                }
                if (type.compareTo("query3") == 0) {}
                if (type.compareTo("query4") == 0) {}
            }
            file.close();
        } catch(IOException e) {
        }
    }
}
