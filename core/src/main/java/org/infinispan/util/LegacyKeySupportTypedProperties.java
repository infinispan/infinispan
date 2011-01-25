package org.infinispan.util;

import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Map;
import java.util.Properties;

/**
 * An extension of TypedProperties that has support for legacy keys.  Construct this Properties type with mappings
 * of currently expected keys to their legacy, deprecated counterparts and you will simply have to look up the new,
 * updated keys and still be able to support the legacy keys, while logging a message that an old key was used. 
 *
 * @author Manik Surtani
 * @version 4.1
 */
public class LegacyKeySupportTypedProperties extends TypedProperties {
   private final Map<String, String> legacyKeyMapping;
   private static final Log log = LogFactory.getLog(LegacyKeySupportTypedProperties.class);

   public LegacyKeySupportTypedProperties(Properties p, Map<String, String> legacyKeyMapping) {
      super(p);
      this.legacyKeyMapping = legacyKeyMapping;
   }

   public LegacyKeySupportTypedProperties(Map<String, String> legacyKeyMapping) {
      this.legacyKeyMapping = legacyKeyMapping;
   }

   private void warnLegacy(String oldKey, String newKey) {
      if (log.isInfoEnabled()) log.info("Could not find value for key %s, but did find value under deprecated key %s. Please use %s as support for %s will eventually be discontinued.", newKey, oldKey);
   }

   @Override
   public String getProperty(String key) {
      if (containsKey(key))
         return super.getProperty(key);
      else {
         String legacyKey = legacyKeyMapping.get(key);
         if (legacyKey == null) return null;
         String val = super.getProperty(legacyKey);
         if (val != null) warnLegacy(legacyKey, key);
         return val;
      }
   }

   @Override
   public String getProperty(String key, String defaultValue) {
      if (containsKey(key))
         return super.getProperty(key);
      else {
         String legacyKey = legacyKeyMapping.get(key);
         if (legacyKey == null) return defaultValue;
         String val = super.getProperty(legacyKey);
         if (val != null) {
            warnLegacy(legacyKey, key);
            return val;
         } else {
            return defaultValue;
         }
      }
   }

   @Override
   public int getIntProperty(String key, int defaultValue) {
      if (containsKey(key))
         return super.getIntProperty(key, defaultValue);
      else {
         String legacyKey = legacyKeyMapping.get(key);
         if (legacyKey == null) return defaultValue;
         if (containsKey(legacyKey)) {
            warnLegacy(legacyKey, key);
            return super.getIntProperty(legacyKey, defaultValue);
         } else {
            return defaultValue;
         }
      }
   }

   @Override
   public long getLongProperty(String key, long defaultValue) {
      if (containsKey(key))
         return super.getLongProperty(key, defaultValue);
      else {
         String legacyKey = legacyKeyMapping.get(key);
         if (legacyKey == null) return defaultValue;
         if (containsKey(legacyKey)) {
            warnLegacy(legacyKey, key);
            return super.getLongProperty(legacyKey, defaultValue);
         } else {
            return defaultValue;
         }
      }
   }

   @Override
   public boolean getBooleanProperty(String key, boolean defaultValue) {
      if (containsKey(key))
         return super.getBooleanProperty(key, defaultValue);
      else {
         String legacyKey = legacyKeyMapping.get(key);
         if (legacyKey == null) return defaultValue;
         if (containsKey(legacyKey)) {
            warnLegacy(legacyKey, key);
            return super.getBooleanProperty(legacyKey, defaultValue);
         } else {
            return defaultValue;
         }
      }
   }
}
