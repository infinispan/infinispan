package org.infinispan.hotrod.test;

import static org.infinispan.hotrod.HotRodServerExtension.builder;

import java.util.concurrent.TimeUnit;

import org.infinispan.api.async.AsyncCache;
import org.infinispan.api.async.AsyncContainer;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.hotrod.HotRodServerExtension;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.RegisterExtension;

/**
 * @since 14.0
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class AbstractSingleHotRodServerTest<K, V> {

   private AsyncContainer container;
   protected AsyncCache<K, V> cache;
   protected String cacheName;

   @RegisterExtension
   static HotRodServerExtension server = builder()
         .build();

   @BeforeEach
   public void setup() throws Exception {
      container = server.getClient().async();
      cacheName = server.cacheName();
      cache = container
            .caches()
            .<K, V>create(cacheName, "test")
            .toCompletableFuture()
            .get(10, TimeUnit.SECONDS);
   }

   @AfterEach
   public void teardown() throws Exception {
      assert container != null : "Container is null";
      assert CompletableFutures.uncheckedAwait(container.caches().remove(cacheName).toCompletableFuture(), 30, TimeUnit.SECONDS) : "Could not delete cache";
      container.close();
   }

}
