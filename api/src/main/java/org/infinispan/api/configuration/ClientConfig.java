package org.infinispan.api.configuration;

import java.util.Properties;

import org.infinispan.api.Experimental;

/**
 * Infinispan Client Config
 *
 * @since 10.0
 */
@Experimental
public interface ClientConfig {
   static ClientConfig from(Properties properties) {
      try {
         return (ClientConfig) ClientConfig.class.getClassLoader()
               .loadClass("org.infinispan.api.client.configuration.InfinispanClientConfigImpl")
               .getConstructor(Properties.class)
               .newInstance(properties);
      } catch (Exception e) {
         e.printStackTrace();
      }
      return null;
   }
}
