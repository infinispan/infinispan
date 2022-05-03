package org.infinispan.hotrod;

import static org.infinispan.hotrod.AwaitAssertions.assertEquals;
import static org.infinispan.hotrod.HotRodServerExtension.builder;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;

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
   public void getPut() throws ExecutionException, InterruptedException {
      try (Infinispan infinispan = server.getClient()) {
         CompletionStage<Object> get = infinispan.async()
               .caches()
               .create("getPut", "test")
               .thenCompose(cache ->
                     cache.set("key", "value")
                           .thenApply(v -> cache)
               )
               .thenCompose(cache -> cache.get("key"));
         assertEquals("value", get);
      }
   }
}
