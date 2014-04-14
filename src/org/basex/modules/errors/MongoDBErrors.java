package org.basex.modules.errors;

import org.basex.query.*;
import org.basex.query.value.item.*;
import org.basex.util.*;

/**
 * This module contains static error functions for the Mongodb module.
 *
 * @author BaseX Team 2005-13, BSD License
 * @author Prakash Thapa
 */
public final class MongoDBErrors {
  /** Error namespace. */
  private static final byte[] NS = QueryText.EXPERROR;
  /** Namespace and error code prefix. */
  private static final String PREFIX =
      new TokenBuilder(QueryText.EXPERR).add(":MONGODB").toString();

  /** Private constructor, preventing instantiation. */
  private MongoDBErrors() { }

  /**
   * MONGODB0001: General Exceptions.
   * @return query exception
   */
  public static QueryException generalExceptionError(final Object e) {
      return thrw(1, "%s", e);
  }
  /**
   * MONGODB0002: JSON format error.
   * @return query exception
   */
  public static QueryException jsonFormatError(final Object e) {
      return thrw(2, "Invalid JSON syntax: '%s'", e);
  }
  /**
   * MONGODB0003: Incorrect username or password.
   * @return query exception
   */
  public static QueryException unAuthorised() {
    return thrw(3, "Invalid username or password");
  }
  /**
   * MONGODB0004: Connection error.
   * @return query exception
   */
  public static QueryException mongoExceptionError(final Object e) {
    return thrw(4, "%s", e);
  }
  /**
   * MONGODB0005: Mongodb handler don't exists.
   * * @param e supplied mongodb Client.
   * @return query exception
   */
  public static QueryException mongoClientError(final Object mongoClient) {
    return thrw(5, "Unknown MongoDB handler: '%s'", mongoClient);
  }
  /**
   * MONGODB0006: Mongodb DB handler don't exists.
   * * @param e MongoDB DB object handler
   * @return query exception
   */
  public static QueryException mongoDBError(final Object db) {
    return thrw(6, "Unknown database handler: '%s'", db);
  }
  /**
   * take two parameters.
   * @param msg
   * @param key
   * @return
   */
  public static QueryException mongoMessageOneKey(final String msg,
          final Object key) {
    return thrw(7, msg, key);
  }
  /**
   * Returns a query exception.
   * @param code code
   * @param msg message
   * @param ext extension
   * @return query exception
   */
  private static QueryException thrw(final int code, final String msg,
      final Object... ext) {
    return new QueryException(null, qname(code), msg, ext);
  }

  /**
   * Creates an error QName for the specified code.
   * @param code code
   * @return query exception
   */
  public static QNm qname(final int code) {
    return new QNm(String.format("%s:MONGO%04d", PREFIX, code), NS);
  }
}
