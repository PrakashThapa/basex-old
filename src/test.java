import org.basex.modules.Couchbase;
import org.basex.query.value.item.Str;


public class test {

    /**
     * @param args
     * @throws Exception 
     */
    public static void main(String[] args) throws Exception {
       Couchbase c = new Couchbase();
      Str s =  c.connection(Str.get("http://127.0.0.1:8091/pools"), Str.get("beer-sample"),Str.get(""));
       //Str d = c.get(s, Str.get("anna"));
       //System.out.println(d.toJava());

    }

}
