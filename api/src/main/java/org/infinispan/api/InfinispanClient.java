package org.infinispan.api;

import org.infinispan.api.configuration.ClientConfig;

/**
 * Entry point for Infinispan API - client
 *
 * @author Katia Aresti, karesti@redhat.com
 * @since 10.0
 */
@Experimental
public class InfinispanClient {

   //TODO: Decide how to test and how to make this work
   public static final Infinispan newInfinispan(ClientConfig config) {
      try {
         return (Infinispan) InfinispanClient.class.getClassLoader()
               .loadClass("org.infinispan.api.client.impl.InfinispanClientImpl")
               .getConstructor(ClientConfig.class)
               .newInstance(config);
      } catch (Exception e) {
      }
      return null;
   }
}
