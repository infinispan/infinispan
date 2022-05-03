package org.infinispan.hotrod;

import static org.infinispan.hotrod.AwaitAssertions.assertAwaitEquals;
import static org.infinispan.hotrod.HotRodServerExtension.builder;

import java.util.concurrent.CompletionStage;

import org.infinispan.api.Infinispan;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

/**
 * @since 14.0
 **/
public class HotRodAsyncCacheTest {

   @RegisterExtension
   static HotRodServerExtension server = builder().build();

   @Test
   public void getPut() {
      try (Infinispan infinispan = server.getClient()) {
         CompletionStage<Object> get = infinispan.async()
               .caches()
               .create("getPut", "test")
               .thenCompose(cache ->
                     cache.set("key", "value")
                           .thenApply(v -> cache)
               )
               .thenCompose(cache -> cache.get("key"));
         assertAwaitEquals("value", get);
      }
   }
}
