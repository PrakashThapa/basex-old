package org.basex.modules;

import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ExecutionException;
import net.spy.memcached.internal.OperationFuture;

import org.basex.query.QueryException;
import org.basex.query.QueryModule;
import org.basex.query.func.FNJson;
import org.basex.query.func.Function;
import org.basex.query.iter.ValueIter;
import org.basex.query.value.Value;
import org.basex.query.value.item.Item;
import org.basex.query.value.item.QNm;
import org.basex.query.value.item.Str;
import org.basex.query.value.map.Map;
import org.basex.query.value.type.SeqType;
import org.basex.util.hash.TokenMap;

import com.couchbase.client.CouchbaseClient;
import com.couchbase.client.CouchbaseConnectionFactory;
import com.couchbase.client.protocol.views.DesignDocument;
import com.couchbase.client.protocol.views.Query;
import com.couchbase.client.protocol.views.View;
import com.couchbase.client.protocol.views.ViewDesign;
import com.couchbase.client.protocol.views.ViewResponse;
import com.couchbase.client.protocol.views.ViewRow;


/**
 * CouchBase extension of Basex.
 *
 * @author BaseX Team 2005-13, BSD License
 * @author Prakash Thapa
 */
public class Couchbase extends QueryModule {
  /** URL of this module. */
  private static final String COUCHBASE_URL = "http://basex.org/modules/couchbase";
  /** QName of Couchbase options. */
  private static final QNm Q_COUCHBASE = QNm.get("couchbase", "options", COUCHBASE_URL);
  
  /**
     * Couchbase instances.
     */

    private HashMap<String, CouchbaseClient> couchbaseclients =
            new HashMap<String, CouchbaseClient>();

    private ArrayList<URI> nodes = new ArrayList<URI>();
    private int timeout = 0;
    
    public Str connection(final Str url, final Str bucket, final Str password) throws Exception {
//        String handler = "Client" + couchbaseclients.size();
//        System.out.println(handler);
//        return Str.get(handler);
        
     	try {
    	    String handler = "Client" + couchbaseclients.size();
    		List<URI> hosts = Arrays.asList(
      		      new URI(url.toJava())
      		    );
    		
//    		CouchbaseConnectionFactory cf = new CouchbaseConnectionFactory(hosts, bucket.toJava(), password.toJava());
//          CouchbaseClient client    =   new CouchbaseClient(cf);
            CouchbaseClient client = new CouchbaseClient(hosts, bucket.toJava(), password.toJava());
		    couchbaseclients.put(handler, client);
		    return Str.get(handler);
    		   
		} catch (Exception ex) {
		    throw new QueryException(ex);
		}
    }
    
    /**
     * get CouchbaseClinet from the hashmap.  
     * @param handler
     * @return
     * @throws QueryException
     */
    private CouchbaseClient getClient(Str handler) throws QueryException {
        String ch = handler.toJava();
        try {
            final CouchbaseClient client = couchbaseclients.get(ch);
            if(client == null)
                throw new QueryException("Unknown CouchbaseClient handler: '" + ch + "'");
            return client;

        } catch (final Exception ex) {
            throw new QueryException(ex);
        }
    }
    
    /**
     * Convert object into xml elements.
     * @param o Object
     * @return xml elements
     * @throws QueryException
     */
    private Item getItemFromObject(Object o) throws QueryException {
        Str json = Str.get(o.toString());
        System.out.println(json);
        return new FNJson(null, Function._JSON_PARSE, json).item(context, null);
    }
    
    private Boolean resultFuture(CouchbaseClient client, OperationFuture<Boolean> result) throws QueryException  {
        try {
            if(result.get().booleanValue()) {
                result.get().toString();
                return true;
            } else {
                throw new QueryException("Could not execute command");
            }
        } catch (InterruptedException e) {
            throw new QueryException(e);
        } catch (ExecutionException e) {
            throw new QueryException(e);
        }
    }
    private Item get(CouchbaseClient client, final Str key) throws QueryException {
        final Object o = client.get(key.toJava());
        final Str json = Str.get(o.toString());
        try {
            return new FNJson(null, Function._JSON_PARSE, json).item(context, null);
        } catch (Exception e) {
          
            throw new QueryException("The result is not in json Format");
        }
          
        
    }
    
    /**
     * add new document.
     * @param handler Database handler
     * @param key
     * @param value
     * @throws QueryException
     */
    public Item add(final Str handler, final Str key, final Str value) throws QueryException {
        CouchbaseClient client = getClient(handler);
        HashMap<String, Object> h = new HashMap<String, Object>();
        TokenMap m = new TokenMap();
        try {
            OperationFuture<Boolean> addResult = client.add(key.toJava(), value.toJava());
            if(addResult.get().booleanValue()){
                return get(client, key);
//               String  o = addResult.get().toString();
//               System.out.println(o);
//                m.put("result",o.toString());
//                h.put("result", o);
//                return getItemFromObject(h);    
            } else {
                throw new QueryException("Could not execute command");
            }
        } catch (InterruptedException e) {
            throw new QueryException(e);
        } catch (ExecutionException ex) {
            throw new QueryException(ex);
        }
    }
    public void set(final Str handler, final Str key,  Str doc) throws QueryException {
        CouchbaseClient client = getClient(handler);
        try {
          OperationFuture<Boolean> result = client.set(key.toJava(), doc.toJava());
          if(result.get().booleanValue()) {
              get(client, key);
          }
          else
              throw new QueryException("Could not execute command");
          
        } catch (Exception ex) {
            throw new QueryException(ex);
        }
         
    }
    /**
     * 
     * @param handler
     * @param key
     * @param doc
     * @throws QueryException
     */
    public void put(final Str handler, final Str key, final Str doc) throws QueryException {
        put(handler, key, doc, null);
    }
    
    public void put(final Str handler, final Str key, final Str doc, final Map options) throws QueryException {
        CouchbaseClient client = getClient(handler);
        try {
          //OperationFuture<Boolean> appendResult = client.append(key.toJava(), doc.toJava());
          if(options != null) {
              Value keys = options.keys();
              for(final Item mapKey : keys) {
                  if(!(key instanceof Str))
                      throw new QueryException("String expected, ...");
                  final String k = ((Str) mapKey).toJava();
                  final Value v = options.get(key, null);
                  if(k.equals("type")){
                      if(v.type().instanceOf(SeqType.STR)) {
                          System.out.println(v.toJava());
                        
                      }
                  }
              }
          } else {
              //OperationFuture<Boolean> result = client.append(key.toJava(), doc.toJava());
          }
          
          
          
        } catch (Exception ex) {
            throw new QueryException(ex);
        }
         
    }
    
    public Item replace(final Str handler,final Str key, final Str doc) throws QueryException {
        CouchbaseClient client = getClient(handler);
        try {
            OperationFuture<Boolean> replaceResult = client.replace(key.toJava(), doc.toJava());
        } catch (Exception ex) {
            throw new QueryException(ex);
        }
       return null;
    }
    
    public Item delete(final Str handler, Str key) throws QueryException {
        CouchbaseClient client = getClient(handler);
        try {
            OperationFuture<Boolean> deleteResult = client.delete(key.toJava());
        } catch (Exception ex) {
            throw new QueryException(ex);
        }
       return null;
    }
    
    
    public Item get(final Str handler, Str key) throws QueryException {
        
        CouchbaseClient client = getClient(handler);
        try {
            final Object o = client.get(key.toJava());
            if(o != null ) {
//                HashMap<String, Object> h = new HashMap<String, Object>();
//                h.put("result", "added successfully");
//                Str x = Str.get(h.toString());
//                System.out.println(x);
//                final Str json = Str.get(o.toString());
//                return new FNJson(null, Function._JSON_PARSE, x).item(context, null); 
                return get(client, key);
            }
            else
                throw new QueryException("Element is empty");
            
            //return new FNJson(null, Function._JSON_PARSE, json).item(context, null);
        } catch (Exception ex) {
            throw new QueryException(ex);
        }
    }
    
 public Str get(final Str handler, Str doc, Map options) throws QueryException {
        CouchbaseClient client = getClient(handler);
        try {
            if(options != null) {
                Value keys = options.keys();
                for(final Item key : keys) {
                    if(!(key instanceof Str))
                        throw new QueryException("String expected, ...");
                    final String k = ((Str) key).toJava();
                    final Value v = options.get(key, null);
                    if(k.equals("add")) {
                        if(v.type().instanceOf(SeqType.STR)) {
                            Str s = (Str) v.toJava();
                            return s;
                        }
                       
                    }
                }
            }
            final Object o = client.get(doc.toJava());
            if(o != null ) {
             //return get(client, doc);
                return null;
            }
            else
                throw new QueryException("Element is empty");
            
            //return new FNJson(null, Function._JSON_PARSE, json).item(context, null);
        } catch (Exception ex) {
            throw new QueryException(ex);
        }
    }
    public Item getText(final Str handler, final Str key) throws QueryException {
        CouchbaseClient client = getClient(handler);
        try {
            OperationFuture<Boolean> result = client.delete(key.toJava());
            resultFuture(client, result);
        } catch (Exception e) {
            throw new QueryException(e);
        }
        return get(client, key);
    }
    public Item remove(final Str handler, final Str key) throws QueryException {
        return delete(handler, key);
    }
    
    
    public Item getBinary(final Str handler, final Str docbin) throws QueryException{
        CouchbaseClient client = getClient(handler);
        
      return null;
    }
    public Item putText(final Str handler, final Str key, final Str value) {
        
        return null;
    }
    public Item putBinary(final Str handler, final Str key, final Str value) {
        
        return null;
    }
    
    
    private String getValueString(Value v) {
        // Str s = (Str) v.toJava();
        String s =  "string";
        //return Str.get("test string");
        return s;
    }
    public Item createView(final Str handler, final Str doc, final String viewName, final Map options) throws QueryException {
        CouchbaseClient client = getClient(handler);
        
        if(options != null) {
            KeyValue k = new KeyValue(options);
            String name = k.get("name");
            String view = k.get("view");
            String map  =    k.get("map");
            DesignDocument designDoc = new DesignDocument(name);
            ViewDesign viewDesign = new ViewDesign(view,map);
            designDoc.getViews().add(viewDesign);
            client.createDesignDoc( designDoc );
        }
       
        return null;
    }
    public Item view(final Str handler, final Str path) {
        
        return view(handler, path, null);
    }
    
    public Item view(final Str handler, final Str path, final Map options) {
        if(options != null) {
            
        }
        return null;
    }
    
    public void shutdown(final Str handler) throws QueryException {
		CouchbaseClient client = getClient(handler);
		client.shutdown();
	}
    
    
    /****** testing methods 
     * @throws QueryException *************************/
    
    public Str test(Map options ) throws QueryException{
        if(options != null) {
            Value keys = options.keys();
            for(final Item key : keys) {
                if(!(key instanceof Str))
                    throw new QueryException("String expected, ...");
                final String k = ((Str) key).toJava();
                final Value v = options.get(key, null);
                if(k.equals("type")) {
                    return (Str) v;
                }
            }
        }
        return null;
    }
    public Item showviews(final Str handler) throws QueryException {
        CouchbaseClient client = getClient(handler);
        View view = client.getView("beer", "brewery_beers");
        Query query = new Query();
        query.setIncludeDocs(true).setLimit(5); // include all docs and limit to 5
        ViewResponse result = client.query(view, query);

        // Iterate over the result and print the key of each document:
       //create json string with key and value
        String json ="";
        for(ViewRow row : result) {
            final String k = row.getKey();
            final String v = row.getValue();
                  
            json = json+ "{"+k+":"+v+"},";
          
          // The full document (as String) is available through row.getDocument();
        }
        
        return new FNJson(null, Function._JSON_PARSE, Str.get(json)).item(context, null);
        
    }
    
    //tests
    public Str getView(final Str handler) throws QueryException {
        CouchbaseClient client = getClient(handler);
        View view = client.getView("Beer", "brewery_beers");
        Query query = new Query();
        ViewResponse result = client.query(view, query);
        Str json = Str.get(result.toString());
        return json;
    }
    
}
