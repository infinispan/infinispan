package org.infinispan.api;

import java.lang.reflect.Method;

/**
 * Entry point for Infinispan API - client
 *
 * @author Katia Aresti, karesti@redhat.com
 * @since 10.0
 */
public class InfinispanClient {

   //TODO: Decide how to test and how to make this work

   public static final Infinispan newInfinispan(ClientConfig config) {
      try {
         return (Infinispan) InfinispanClient.class.getClassLoader()
               .loadClass("org.infinispan.api.client.impl.InfinispanClientImpl")
               .getConstructor(ClientConfig.class)
               .newInstance(config);
      } catch (Exception e) {
         e.printStackTrace();
      }
      return null;
   }

   public static Infinispan newInfinispan() {
      try {
         Method defaultClientConfig = InfinispanClient.class.getClassLoader()
               .loadClass("org.infinispan.api.client.impl.ClientConfigurationLoader")
               .getMethod("defaultClientConfig");
         ClientConfig config = (ClientConfig) defaultClientConfig.invoke(" staticMethod");
         return newInfinispan(config);
      } catch (Exception e) {
         e.printStackTrace();
      }
      return null;
   }

}
