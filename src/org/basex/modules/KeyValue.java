package org.basex.modules;

import org.basex.query.QueryContext;
import org.basex.query.QueryException;
import org.basex.query.value.Value;
import org.basex.query.value.item.FItem;
import org.basex.query.value.item.Item;
import org.basex.query.value.item.QNm;
import org.basex.query.value.item.Str;
import org.basex.query.value.map.Map;
import org.basex.query.value.node.FElem;
import org.basex.query.value.type.FuncType;
import org.basex.util.InputInfo;

public class KeyValue {

    public  KeyValue(Map option) {
        options = option;
    }
    /**
     * @param args
     */
    private Map options;
    private String getValueString(Value v) {
        // Str s = (Str) v.toJava();
        String s =  "string";
        return s;
    }
    public String get(String val) throws QueryException {
        String value = null;
        if(options != null) {
            Value keys = options.keys();
            for(final Item key : keys) {
                if(!(key instanceof Str))
                    throw new QueryException("String expected, ...");
                final String k = ((Str) key).toJava();
                final Value v = options.get(key, null);
               if(k.equals(val)) {
                   value = getValueString(v);
               }
            }
        }
      return value;
    }
   

}
