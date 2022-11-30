package org.infinispan.tools.store.migrator;

import static org.infinispan.tools.store.migrator.Element.CACHE_NAME;
import static org.infinispan.tools.store.migrator.Element.MARSHALLER;
import static org.infinispan.tools.store.migrator.Element.SEGMENT_COUNT;
import static org.infinispan.tools.store.migrator.Element.TARGET;
import static org.infinispan.tools.store.migrator.Element.TYPE;
import static org.infinispan.tools.store.migrator.Element.VERSION;

import java.util.Properties;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.util.Version;

public class StoreProperties{

   private final Element root;
   private final Properties properties;
   private final StoreType storeType;
   private final String cacheName;
   private final int majorVersion;

   public StoreProperties(StoreProperties properties) {
      this.root = properties.root;
      this.storeType = properties.storeType;
      this.cacheName = properties.cacheName();
      this.majorVersion = properties.majorVersion();

      Properties clone = new Properties(properties.properties.size());
      clone.putAll(properties.properties);
      this.properties = clone;
   }

   public StoreProperties(Element root, Properties properties) {
      this.root = root;
      this.properties = properties;
      validate();
      this.storeType = StoreType.valueOf(get(TYPE).toUpperCase());
      this.cacheName = get(CACHE_NAME);
      this.majorVersion = majorVersion();
   }

   private int majorVersion() {
      String version = get(VERSION);
      if (version != null)
         return Integer.parseInt(version);

      return Integer.parseInt(Version.getMajor());
   }

   public boolean isTargetStore() {
      return root == TARGET;
   }

   public int getMajorVersion() {
      return majorVersion;
   }

   public boolean isCurrentMajorVersion() {
      return Integer.parseInt(Version.getMajor()) == majorVersion;
   }

   public String cacheName() {
      return cacheName;
   }

   public StoreType storeType() {
      return storeType;
   }

   public boolean isSegmented() {
      String segmentCount = get(SEGMENT_COUNT);
      if (segmentCount == null)
         return false;
      return Integer.parseInt(segmentCount) > 0;
   }

   public void put(String value, Element... elements) {
      properties.put(key(elements), value);
   }

   public String get(Element... elements) {
      return properties.getProperty(key(elements));
   }

   public String key(Element... elements) {
      StringBuilder sb = new StringBuilder(root.toString().toLowerCase());
      sb.append(".");
      for (int i = 0; i < elements.length; i++) {
         sb.append(elements[i].toString());
         if (i != elements.length - 1) sb.append(".");
      }
      return sb.toString();
   }

   public void required(String... required) {
      for (String prop : required) {
         if (properties.get(prop) == null) {
            String msg = String.format("The property %s must be specified.", prop);
            throw new CacheConfigurationException(msg);
         }
      }
   }

   public void required(Element... required) {
      for (Element prop : required) {
         if (properties.get(key(prop)) == null) {
            String msg = String.format("The property '%s' must be specified.", key(prop));
            throw new CacheConfigurationException(msg);
         }
      }
   }

   private void validate() {
      required(TYPE);
      required(CACHE_NAME);

      if (get(MARSHALLER, TYPE) != null) {
         throw new CacheConfigurationException(String.format("Property '%s' has been removed, please specify %s instead.",
               key(MARSHALLER, TYPE), key(VERSION)));
      }
   }
}
