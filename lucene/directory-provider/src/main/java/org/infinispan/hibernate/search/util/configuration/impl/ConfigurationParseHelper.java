package org.infinispan.hibernate.search.util.configuration.impl;

import org.hibernate.search.exception.SearchException;
import org.hibernate.search.util.StringHelper;
import org.infinispan.hibernate.search.impl.LoggerFactory;
import org.infinispan.hibernate.search.logging.Log;

import java.util.Properties;

/**
 * Provides functionality for dealing with configuration values.
 *
 * @author Sanne Grinovero
 * @author Steve Ebersole
 * @author Emmanuel Bernard <emmanuel@hibernate.org>
 * @author Hardy Ferentschik
 */
public class ConfigurationParseHelper {

   private static final Log log = LoggerFactory.make();

   private ConfigurationParseHelper() {
   }

   /**
    * Retrieves a configuration property and parses it as an Integer if it exists, or returns null if the property is
    * not set (undefined).
    *
    * @param cfg configuration Properties
    * @param key the property key
    * @return the Integer or null
    * @throws SearchException both for empty (non-null) values and for Strings not containing a valid int
    *                         representation.
    */
   public static Integer getIntValue(Properties cfg, String key) {
      String propValue = cfg.getProperty(key);
      if (propValue == null) {
         return null;
      }
      if (StringHelper.isEmpty(propValue.trim())) {
         throw log.configurationPropertyCantBeEmpty(key);
      } else {
         return parseInt(propValue, 0, "Unable to parse " + key + ": " + propValue);
      }
   }

   /**
    * In case value is null or an empty string the defValue is returned
    *
    * @param value
    * @param defValue
    * @param errorMsgOnParseFailure
    * @return the converted int.
    * @throws SearchException if value can't be parsed.
    */
   public static final int parseInt(String value, int defValue, String errorMsgOnParseFailure) {
      if (StringHelper.isEmpty(value)) {
         return defValue;
      } else {
         return parseInt(value, errorMsgOnParseFailure);
      }
   }

   /**
    * Parses a string into an integer value.
    *
    * @param value                  a string containing an int value to parse
    * @param errorMsgOnParseFailure message being wrapped in a SearchException if value is {@code null} or not an
    *                               integer
    * @return the parsed integer value
    * @throws SearchException both for null values and for Strings not containing a valid int.
    */
   public static int parseInt(String value, String errorMsgOnParseFailure) {
      if (value == null) {
         throw new SearchException(errorMsgOnParseFailure);
      } else {
         try {
            return Integer.parseInt(value.trim());
         } catch (NumberFormatException nfe) {
            throw log.getInvalidIntegerValueException(errorMsgOnParseFailure, nfe);
         }
      }
   }

   /**
    * Extracts a boolean value from configuration properties
    *
    * @param cfg          configuration Properties
    * @param key          the property key
    * @param defaultValue a boolean.
    * @return the defaultValue if the property was not defined
    * @throws SearchException for invalid format or values.
    */
   public static final boolean getBooleanValue(Properties cfg, String key, boolean defaultValue) {
      String propValue = cfg.getProperty(key);
      if (propValue == null) {
         return defaultValue;
      } else {
         return parseBoolean(propValue, "Property '" + key + "' needs to be either literal 'true' or 'false'");
      }
   }

   /**
    * Parses a string to recognize exactly either "true" or "false".
    *
    * @param value                  the string to be parsed
    * @param errorMsgOnParseFailure the message to be put in the exception if thrown
    * @return true if value is "true", false if value is "false"
    * @throws SearchException for invalid format or values.
    */
   public static final boolean parseBoolean(String value, String errorMsgOnParseFailure) {
      // avoiding Boolean.valueOf() to have more checks: makes it easy to spot wrong type in cfg.
      if (value == null) {
         throw new SearchException(errorMsgOnParseFailure);
      } else if ("false".equalsIgnoreCase(value.trim())) {
         return false;
      } else if ("true".equalsIgnoreCase(value.trim())) {
         return true;
      } else {
         throw new SearchException(errorMsgOnParseFailure);
      }
   }

   /**
    * Get the string property or defaults if not present
    */
   public static final String getString(Properties cfg, String key, String defaultValue) {
      if (cfg == null) {
         return defaultValue;
      } else {
         String propValue = cfg.getProperty(key);
         return propValue == null ? defaultValue : propValue;
      }
   }

}
