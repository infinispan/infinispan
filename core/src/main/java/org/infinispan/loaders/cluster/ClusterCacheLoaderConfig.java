package org.infinispan.loaders.cluster;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Properties;

import org.infinispan.config.parsing.XmlConfigHelper;
import org.infinispan.loaders.AbstractCacheLoaderConfig;
import org.infinispan.util.TypedProperties;

/**
 * Configuration for {@link org.infinispan.loaders.cluster.ClusterCacheLoader}.
 *
 * @author Mircea.Markus@jboss.com
 */
public class ClusterCacheLoaderConfig extends AbstractCacheLoaderConfig {

   private static final long serialVersionUID = -44748358960849539L;
   
   private long remoteCallTimeout;
   
   protected TypedProperties properties = EMPTY_PROPERTIES;

   public Properties getProperties() {
      return properties;
   }

   public void setProperties(Properties properties) {
      testImmutability("properties");
      this.properties = toTypedProperties(properties);
   }

   public void setProperties(String properties) throws IOException {
      if (properties == null) return;

      testImmutability("properties");
      // JBCACHE-531: escape all backslash characters
      // replace any "\" that is not preceded by a backslash with "\\"
      properties = XmlConfigHelper.escapeBackslashes(properties);
      ByteArrayInputStream is = new ByteArrayInputStream(properties.trim().getBytes("ISO8859_1"));
      this.properties = new TypedProperties();
      this.properties.load(is);
      is.close();
   }

   public String getCacheLoaderClassName() {
      return ClusterCacheLoader.class.getName();
   }

   public long getRemoteCallTimeout() {
      return remoteCallTimeout;
   }

   public void setRemoteCallTimeout(long remoteCallTimeout) {
      testImmutability("remoteCallTimeout");
      this.remoteCallTimeout = remoteCallTimeout;
   }
}
