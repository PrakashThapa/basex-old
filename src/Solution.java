import org.basex.modules.MongoDB;
import org.basex.query.QueryException;
import org.basex.query.value.item.Item;
import org.basex.query.value.item.Str;

import com.mongodb.DB;
import com.mongodb.DBObject;


public class Solution extends MongoDB {
    public  Solution() {
    }
    /**
     * s.
     * @param handler
     * @return
     * @throws QueryException
     */
    public Item q(final Str handler) throws QueryException{
    final DB db = getDbHandler(handler);
    Str s = Str.get("{_id:1,name:1}");
    DBObject d = getDbObjectFromStr(s);
    return null;
}
}
