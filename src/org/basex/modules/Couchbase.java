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
import org.basex.query.value.item.Item;
import org.basex.query.value.item.QNm;
import org.basex.query.value.item.Str;
import org.basex.util.hash.TokenMap;


import com.couchbase.client.CouchbaseClient;
import com.couchbase.client.protocol.views.Query;
import com.couchbase.client.protocol.views.View;
import com.couchbase.client.protocol.views.ViewResponse;
import com.couchbase.client.protocol.views.ViewRow;
import com.mongodb.util.JSON;


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
    
    public Str connection(final Str url, final Str bucket, final Str password) throws Exception {
     	try {
    	    String handler = "Client" + couchbaseclients.size();
    		List<URI> hosts = Arrays.asList(
      		      new URI(url.toJava())
      		    );
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
     * convert object into xml elements
     * @param o Object
     * @return xml elements
     * @throws QueryException
     */
    private Item getItemFromObject(Object o) throws QueryException {
        Str json = Str.get(o.toString());
        System.out.println(json);
        return new FNJson(null, Function._JSON_PARSE, json).item(context, null);
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
            OperationFuture<Boolean> s = client.add(key.toJava(), value.toJava());
            Object  o = s.get();
            m.put("result",o.toString());
            h.put("result", o);
            return getItemFromObject(h);
          
        } catch (InterruptedException e) {
            throw new QueryException(e);
        } catch (ExecutionException ex) {
            throw new QueryException(ex);
        }
    }
    public void set(final Str handler, Str doc) throws QueryException {
        CouchbaseClient client = getClient(handler);
        try {
           
           client.set(doc.toJava(),"test");
           
        } catch (Exception ex) {
            throw new QueryException(ex);
        }
         
    }
    
    public Item get(final Str handler, Str doc) throws QueryException {
        
        CouchbaseClient client = getClient(handler);
        try {
            Object o = client.get(doc.toJava());
            Str json = Str.get(o.toString());
            System.out.println(json);
            return new FNJson(null, Function._JSON_PARSE, json).item(context, null);
        } catch (Exception ex) {
            throw new QueryException(ex);
        }
    }
    public void shutdown(final Str handler) throws QueryException {
		CouchbaseClient client = getClient(handler);
		client.shutdown();
	}
}
