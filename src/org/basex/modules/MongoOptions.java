package org.basex.modules;

import java.util.Locale;

import org.basex.build.JsonOptions;
import org.basex.util.options.EnumOption;
import org.basex.util.options.NumberOption;
import org.basex.util.options.OptionsOption;
import org.basex.util.options.StringOption;
/**
 * Options for MongoDB.
 *
 * @author BaseX Team 2005-13, BSD License
 * @author Prakash Thapa
 */
public class MongoOptions extends JsonOptions {
    /** MongoDB host. */
    public static final StringOption HOST = new StringOption("host");
    /** MongoDB port option. */
    public static final NumberOption PORT = new NumberOption("port");
    public static final StringOption USERNAME = new StringOption("user");
    public static final StringOption PASSWORD = new StringOption("password");
    public static final OptionsOption<MongoOptions> TYPEN =
        new OptionsOption<MongoOptions>("type", new MongoOptions());
    public static final EnumOption<MongoFormat> TYPE =
            new EnumOption<MongoFormat>("type", MongoFormat.XML);
    public static final OptionsOption<JsonOptions> JSON =
            new OptionsOption<JsonOptions>("json", new JsonOptions());
    /** return result type */
    public enum MongoFormat {
      /** json. */ JSON,
      /** xml. */ XML;

      @Override
      public String toString() {
        return super.toString().toLowerCase(Locale.ENGLISH);
      }
    }
}
