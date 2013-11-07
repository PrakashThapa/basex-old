package org.basex.modules;

import org.basex.build.JsonOptions;
import org.basex.build.JsonOptions.JsonFormat;
import org.basex.build.JsonParserOptions;
import org.basex.io.parse.json.JsonConverter;
import org.basex.modules.NosqlOptions.NosqlFormat;
import org.basex.query.QueryException;
import org.basex.query.QueryIOException;
import org.basex.query.QueryModule;
import org.basex.query.func.FNJson;
import org.basex.query.func.FuncOptions;
import org.basex.query.func.Function;
import org.basex.query.value.item.Item;
import org.basex.query.value.item.QNm;
import org.basex.query.value.item.Str;
import org.basex.query.value.node.ANode;
import org.basex.query.value.node.FElem;

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
//        final JsonParserOptions jopts = new JsonParserOptions();
//        Object pt = opt.get(NosqlOptions.FORMAT2);
//        new FuncOptions(qnmOptions, null).parse((Item) pt, jopts);
            try {
                if(opt != null) {
                    if(opt.get(NosqlOptions.TYPE) == NosqlFormat.XML) {
                        final JsonParserOptions opts = new JsonParserOptions();
                        opts.set(JsonOptions.FORMAT, opt.get(JsonOptions.FORMAT));
                        return JsonConverter.convert(json.string(), opts);

                        //return new FNJson(staticContext, null,
                        //        Function._JSON_PARSE, json).
                        //        item(queryContext, null);
                    } else {
                        return json;
                        //just change for formatting of josn
//                        Item xXml = new FNJson(staticContext, null,
//                                Function._JSON_PARSE, json).
//                                item(queryContext, null);
//                        return new FNJson(staticContext, null,
//                                Function._JSON_SERIALIZE, xXml).
//                                item(queryContext, null);
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
}
