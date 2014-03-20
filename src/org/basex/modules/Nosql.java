package org.basex.modules;

import java.util.List;

import org.basex.build.JsonOptions;
import org.basex.build.JsonParserOptions;
import org.basex.io.parse.json.JsonConverter;
import org.basex.io.serial.SerialMethod;
import org.basex.io.serial.SerializerOptions;
import org.basex.modules.NosqlOptions.NosqlFormat;
import org.basex.query.QueryException;
import org.basex.query.QueryModule;
import org.basex.query.func.FNJson;
import org.basex.query.func.FuncOptions;
import org.basex.query.func.Function;
import org.basex.query.value.item.Item;
import org.basex.query.value.item.QNm;
import org.basex.query.value.item.Str;
import org.basex.query.value.map.Map;
import org.basex.query.value.node.ANode;
import org.basex.query.value.node.FElem;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

/**
 * All Nosql database common functionality.
 *
 * @author BaseX Team 2005-13, BSD License
 * @author Prakash Thapa
 */
class Nosql extends QueryModule {
    private  QNm qnmOptions;
    public Nosql(final QNm qnM) {
        qnmOptions   =   qnM;
    }
    protected String addchar(final String v, final int index, final char check,
            final char replace) {
        String val = v.trim();
        StringBuilder s = new StringBuilder();
        if(val.charAt(index) != check) {
            s.append(replace);
        }
     return s.toString();
    }
    public String jsonString(final String v) {
        StringBuilder s = new StringBuilder();
        String val = v.trim();
        if(val.isEmpty()) {
            return "";
        }
        if(val.charAt(0) == '"') {
        } else {
            s.append('"');
        }
        s.append(val);
        if(val.charAt(val.length() - 1) == '"') {
        } else {
            s.append('"');
        }
        return s.toString().trim();
    }
    public String jsonString(final String k, final String v) {
        StringBuilder s = new StringBuilder();
        s.append(jsonString(k)).append(":").append(jsonString(v));
        return s.toString();
    }
    public String jsonString(final String k, final List<String> list,
            final boolean object) {
        StringBuilder s = new StringBuilder();
        s.append(" { ");
        if(list.size() == 1) {
            s.append(jsonString(k)).append(" : ");
            for(String v: list) {
                s.append(jsonString(v));
            }
        } else {
            s.append(jsonString(k)).append(" : ");
            s.append("[ ");
            for(String v: list) {
                if(s.length() > 2) s.append(", ");
                s.append(jsonString(v));
            }
            s.append(" ] ");
        }
        s.append(" } ");
        return s.toString();
    }
    public String jsonString(final String k, final List<String> list) {
        StringBuilder s = new StringBuilder();
        if(list.size() == 1) {
            s.append(jsonString(k)).append(" : ");
            for(String v: list) {
                s.append(jsonString(v));
            }
        } else {
            s.append(jsonString(k)).append(" : ");
            s.append("[ ");
            for(String v: list) {
                if(s.length() > 2) s.append(", ");
                s.append(jsonString(v));
            }
            s.append(" ] ");
        }
        return s.toString();
    }
    public String jsonString(final String k, final String v, final boolean object) {
        StringBuilder s = new StringBuilder();
        String val = v.trim();
        if(val.charAt(0) != '{') {
            s.append('{');
        }
        s.append(jsonString(val));
        if(val.charAt(val.length() - 1) != '}') {
            s.append('}');
        }
        return s.toString();
    }
    /**
     * return java String from Item.
     * @param item
     * @return
     * @throws QueryException
     */
    protected String itemToString(final Item item) throws QueryException {
        if(item instanceof Str) {
            try {
                String string = ((Str) item).toJava();
                return string;
            } catch (Exception e) {
                throw new QueryException("Item is not in well format");
            }
        } else {
            throw new QueryException("Item is not in Str format");
        }
    }
    protected Item formatjson(final Str json,  final NosqlOptions jopts)
            throws QueryException {
        final SerializerOptions sopts = new SerializerOptions();
        sopts.set(SerializerOptions.METHOD, SerialMethod.JSON);
        Item x = new FNJson(staticContext, null, Function.SERIALIZE,
                json).item(queryContext, null);
        return x;
    }
    /**
     * return java String from Item.
     * @param item
     * @return
     * @throws QueryException
     */
    protected String itemToJsonString(final Item item) throws QueryException {
        if(item instanceof Str) {
            try {
                boolean jsoncheck = checkJson(item);
                if(jsoncheck) {
                    String string = ((Str) item).toJava();
                    return string;
                }
            } catch (Exception e) {
                throw new QueryException("Item is not in well format");
            }
        } else {
            throw new QueryException("Item is not in Str format");
        }
        return null;
    }
    /**
     * Check if the string is json format or not. if not throw exception
     * @param doc
     * @throws QueryException
     */
    protected boolean checkJson(final Item doc) throws QueryException {
        try {
            new FNJson(staticContext, null, Function._JSON_PARSE,
                    doc).item(queryContext, null);
            return true;
        } catch (Exception e) {
            throw new QueryException("document is not in json format");
        }
    }
    /**
     * This is the final result return.
     * @param handler
     * @param json
     * @return
     * @throws QueryException
     */
    protected Item finalResult(final Str json, final NosqlOptions opt)
            throws Exception {
            try {
                if(opt != null) {
                    if(opt.get(NosqlOptions.TYPE) == NosqlFormat.XML) {
                        final JsonParserOptions opts = new JsonParserOptions();
                        opts.set(JsonOptions.FORMAT, opt.get(JsonOptions.FORMAT));
                        final JsonConverter conv = JsonConverter.get(opts);
                        conv.convert(json.string(), null);
                        return conv.finish();
                    } else {
                        Item xXml = new FNJson(staticContext, null,
                                Function._JSON_PARSE, json).
                                item(queryContext, null);
                        return new FNJson(staticContext, null,
                                Function._JSON_SERIALIZE, xXml).
                                item(queryContext, null);
                    }
                }
                return new FNJson(staticContext, null, Function._JSON_PARSE, json).
                        item(queryContext, null);
            } catch (final Exception ex) {
                throw new QueryException(ex);
            }
    }
    /**
     * this is just test method.
     * @param json
     * @param opt
     * @return
     * @throws QueryException
     */
    protected Item test(final Str json, final NosqlOptions opt)
            throws QueryException {
        final JsonParserOptions jopts = new JsonParserOptions();
        new FuncOptions(qnmOptions, null).parse((Item) json, jopts);
            try {
                if(opt != null) {
                    if(opt.get(NosqlOptions.TYPE) == NosqlFormat.XML) {
                        return new FNJson(staticContext, null,
                                Function._JSON_PARSE, json).
                                item(queryContext, null);
                    } else {
                        //return json;
                        //just change for formatting of josn
                        Item xXml = new FNJson(staticContext, null,
                                Function._JSON_PARSE, json).
                                item(queryContext, null);
                        return new FNJson(staticContext, null,
                                Function._JSON_SERIALIZE, xXml).
                                item(queryContext, null);
                    }
                }
                return new FNJson(staticContext, null, Function._JSON_PARSE, json).
                        item(queryContext, null);
            } catch (final Exception ex) {
                throw new QueryException(ex);
            }
    }
    /**
     * add type in each element.
     * @param node
     * @param container
     * @return
     */
    protected ANode addtype(final ANode node, final FElem container) {
        if(node.hasChildren()) {
            ((FElem) node).add("type", "object");
            for(Item child:((FElem) node).children()) {
                FElem newNode = (FElem) child;
                if(newNode.hasChildren()) {
                    FElem x = null;
                    container.add(addtype(newNode, x));
                } else {
                    newNode.add("type", newNode.type.string());
                    container.add(newNode);
                }
            }
        } else {
            ((FElem) node).add("type", node.string());
            container.add(node);
        }
        return container;
    }
    public Item nodetojson2(final Item node) throws QueryException {
        final FElem jsonNode = new FElem("json");
        jsonNode.add("type", "object");
       ANode x = addtype((ANode) node, jsonNode);
//        Item json = new FNJson(null, null, Function._JSON_SERIALIZE, jsonNode).
//                item(queryContext, null);
        return x;
    }
    protected Item node(final Item item) throws QueryException {
        final FElem jsonNode = new FElem("json");
        jsonNode.add("type", "object");
        jsonNode.add((ANode) item);
        return jsonNode;
    }
    /**** to json. **/
    public Object toJson(final String key, final Object value) {
        if(value != null) {
            JSONObject json = new JSONObject();
            try {
                json.put(key, value);
            } catch (JSONException e) {
            }
            return json;
        } else {
            JSONArray json = new JSONArray();
            json.put(key);
            return json;
        }
    }
    public Object stringToJsonObject(final Object string) {
        String s;
        if(string instanceof Str) {
            s = ((Str) string).toJava();
        } else {
            s = (String) string;
        }
        try {
            JSONObject json = new JSONObject(s);
            return json;
        } catch (JSONException e) {
            NosqlErrors.generalExceptionError(e);
        }
        return null;
    }
   protected Map insert(final Map m, final String k, final String v)
           throws QueryException {
       m.insert(Str.get(k), Str.get(v), null);
       return m;
   }
}
