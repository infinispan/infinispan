package org.infinispan.util;

import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * A wrapper around system properties that supports legacy keys
 *
 * @author Manik Surtani
 * @version 4.1
 */
public class LegacyKeySupportSystemProperties {

   private static final Log log = LogFactory.getLog(LegacyKeySupportSystemProperties.class);

   private static void warnLegacy(String oldKey, String newKey) {
      if (log.isInfoEnabled()) log.info("Could not find value for key %1$s, but did find value under deprecated key %2$s. Please use %1$s as support for %2$s will eventually be discontinued.", newKey, oldKey);
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
