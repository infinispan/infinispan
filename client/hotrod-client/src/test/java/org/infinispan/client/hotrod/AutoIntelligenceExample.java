package org.infinispan.client.hotrod;

import org.infinispan.client.hotrod.configuration.ClientIntelligence;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;

/**
 * Example demonstrating how to use AUTO client intelligence mode.
 *
 * AUTO mode starts as HASH_DISTRIBUTION_AWARE for optimal performance,
 * but automatically degrades to BASIC mode if topology servers become
 * unreachable. This is particularly useful in environments where:
 * - Containers run on non-Linux platforms
 * - NAT is involved
 * - Server topology may not be directly accessible
 *
 * @since 16.4
 */
public class AutoIntelligenceExample {

   public static void main(String[] args) {
      // Configure client with AUTO intelligence mode
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder
         .addServer()
            .host("127.0.0.1")
            .port(11222)
         // Set client intelligence to AUTO
         .clientIntelligence(ClientIntelligence.AUTO);

      // Create the remote cache manager
      RemoteCacheManager cacheManager = new RemoteCacheManager(builder.build());

      try {
         // Get the default cache
         RemoteCache<String, String> cache = cacheManager.getCache();

         // Initially, the client will use HASH_DISTRIBUTION_AWARE mode
         // for optimal routing to primary owners
         cache.put("key1", "value1");
         System.out.println("Value: " + cache.get("key1"));

         // If topology servers become unreachable, the client will
         // automatically degrade to BASIC mode and continue operations
         // using the initially configured server list

         cache.put("key2", "value2");
         System.out.println("Value: " + cache.get("key2"));

      } finally {
         cacheManager.stop();
      }
   }

   /**
    * Example of programmatically checking the intelligence mode.
    */
   public static void demonstrateIntelligenceChecks() {
      // Check if a mode is topology-aware
      boolean isAwareAuto = ClientIntelligence.AUTO.isTopologyAware(); // true
      boolean isAwareBasic = ClientIntelligence.BASIC.isTopologyAware(); // false

      // Get the effective intelligence level
      ClientIntelligence effective = ClientIntelligence.AUTO.getEffectiveIntelligence();
      // Returns HASH_DISTRIBUTION_AWARE

      System.out.println("AUTO is topology-aware: " + isAwareAuto);
      System.out.println("BASIC is topology-aware: " + isAwareBasic);
      System.out.println("AUTO effective intelligence: " + effective);
   }
}
