package org.infinispan.util;

import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Properties;

/**
 * A wrapper around system properties that supports legacy keys
 *
 * @author Manik Surtani
 * @version 4.1
 */
public class LegacyKeySupportSystemProperties {

   private static final Log log = LogFactory.getLog(LegacyKeySupportSystemProperties.class);

   private static void warnLegacy(String oldKey, String newKey) {
      if (log.isInfoEnabled()) log.info("Could not find value for key {0}, but did find value under deprecated key {1}. Please use {0} as support for {1} will eventually be discontinued.", newKey, oldKey);
   }

   public static String getProperty(String key, String legacyKey) {
      String val = System.getProperty(key);
      if (val == null) {
         val = System.getProperty(legacyKey);
         if (val != null) warnLegacy(legacyKey, key);
      }
      return val;
   }

   public static String getProperty(String key, String legacyKey, String defaultValue) {
      String val = System.getProperty(key);
      if (val == null) {
         val = System.getProperty(legacyKey);
         if (val != null)
            warnLegacy(legacyKey, key);
         else
            val = defaultValue;
      }
      return val;
   }   
}
