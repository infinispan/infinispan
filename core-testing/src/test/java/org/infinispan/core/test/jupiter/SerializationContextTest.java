package org.infinispan.core.test.jupiter;

import static org.assertj.core.api.Assertions.assertThat;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.core.test.jupiter.proto.Product;
import org.infinispan.core.test.jupiter.proto.TestProductSCI;
import org.junit.jupiter.api.Test;

/**
 * Validates that ProtoStream SerializationContextInitializer registration works
 * and proto entities can be stored and retrieved across cluster nodes.
 */
@InfinispanCluster(numNodes = 2, serializationContext = TestProductSCI.class)
class SerializationContextTest {

   @InfinispanResource
   InfinispanContext ctx;

   @Test
   void testProtoEntityReplication() {
      var cache = ctx.<String, Product>createCache(b ->
            b.clustering().cacheMode(CacheMode.REPL_SYNC));

      Product laptop = new Product("Laptop", 999.99);
      cache.on(0).put("laptop", laptop);

      Product retrieved = cache.on(1).get("laptop");
      assertThat(retrieved).isNotNull();
      assertThat(retrieved.getName()).isEqualTo("Laptop");
      assertThat(retrieved.getPrice()).isEqualTo(999.99);
   }

   @Test
   void testProtoEntityDistribution() {
      var cache = ctx.<String, Product>createCache(b ->
            b.clustering().cacheMode(CacheMode.DIST_SYNC));

      Product phone = new Product("Phone", 599.0);
      cache.on(0).put("phone", phone);

      Product retrieved = cache.on(1).get("phone");
      assertThat(retrieved).isNotNull();
      assertThat(retrieved.getName()).isEqualTo("Phone");
      assertThat(retrieved.getPrice()).isEqualTo(599.0);
   }
}
