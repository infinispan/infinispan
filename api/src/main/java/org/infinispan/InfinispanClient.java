package org.infinispan;

import java.lang.reflect.Method;

public class InfinispanClient {

   public static final Infinispan newInfinispan(ClientConfig config) {
      try {
         return (Infinispan) InfinispanClient.class.getClassLoader()
               .loadClass("org.infinispan.api.impl.InfinispanClientImpl")
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
               .loadClass("org.infinispan.client.hotrod.configuration.Configuration")
               .getMethod("defaultClientConfig");
         ClientConfig config = (ClientConfig) defaultClientConfig.invoke(" staticMethod");
         return newInfinispan(config);
      } catch (Exception e) {
         e.printStackTrace();
      }
      return null;
   }

}
