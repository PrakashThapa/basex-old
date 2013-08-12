import org.basex.modules.Old_MongoDB;
import org.basex.modules.MongoDB2;
import org.basex.query.QueryException;
import org.basex.query.value.item.Str;
import org.basex.util.Performance;


public class test {

    /**
     * @param args
     * @throws QueryException 
     */
    public static void main(String[] args) throws QueryException {
        
        MongoDB2 d = new MongoDB2();
        String s = "mongodb://localhost/test";
        Performance pef = new Performance();
//        d.find(Str.get(s), Str.get(""), Str.get("bias"));
//        System.out.println(pef);
    
    }

}
