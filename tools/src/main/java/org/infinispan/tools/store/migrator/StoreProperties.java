package org.infinispan.tools.store.migrator;

import static org.infinispan.tools.store.migrator.Element.CACHE_NAME;
import static org.infinispan.tools.store.migrator.Element.MARSHALLER;
import static org.infinispan.tools.store.migrator.Element.TARGET;
import static org.infinispan.tools.store.migrator.Element.TYPE;
import static org.infinispan.tools.store.migrator.Element.VERSION;

import java.util.Properties;

import org.infinispan.Version;
import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.tools.store.migrator.marshaller.MarshallerType;

public class StoreProperties{

   private final Element root;
   private final Properties properties;
   private final StoreType storeType;
   private final String cacheName;
   private final int majorVersion;

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

      String marshallerTypeProp = get(MARSHALLER, TYPE);
      if (marshallerTypeProp != null)
         return MarshallerType.valueOf(get(MARSHALLER, TYPE).toUpperCase()).getMajorVersion();

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
   }
}
