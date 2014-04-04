package org.infinispan.commons.util;

import org.infinispan.commons.logging.Log;
import org.infinispan.commons.logging.LogFactory;

/**
 * A wrapper around system properties that supports legacy keys
 *
 * @author Manik Surtani
 * @version 4.1
 */
public class LegacyKeySupportSystemProperties {

   private static final Log log = LogFactory.getLog(LegacyKeySupportSystemProperties.class);

   private static void warnLegacy(String oldKey, String newKey) {
      if (log.isInfoEnabled())
         log.infof("Could not find value for key %1$s, but did find value under deprecated key %2$s. Please use %1$s as support for %2$s will eventually be discontinued.",
                  newKey, oldKey);
   }

   public static String getProperty(String key, String legacyKey) {
      String val = SecurityActions.getProperty(key);
      if (val == null) {
         val = SecurityActions.getProperty(legacyKey);
         if (val != null) warnLegacy(legacyKey, key);
      }
      return val;
   }

   public static String getProperty(String key, String legacyKey, String defaultValue) {
      String val = SecurityActions.getProperty(key);
      if (val == null) {
         val = SecurityActions.getProperty(legacyKey);
         if (val != null)
            warnLegacy(legacyKey, key);
         else
            val = defaultValue;
      }
      return val;
   }   
}
