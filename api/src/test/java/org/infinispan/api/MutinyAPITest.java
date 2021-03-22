package org.infinispan.api;

import org.infinispan.api.mutiny.MutinyCache;

import io.smallrye.mutiny.Uni;

/**
 * @since 13.0
 **/
public class MutinyAPITest {
   public void testAPI() {
      try (Infinispan infinispan = Infinispan.create("file:///path/to/infinispan.xml")) {
         Uni<MutinyCache<String, String>> uni = infinispan.mutiny().caches().cache("mycache");

         uni.chain(c -> c.query("where a > 5").param("","").delete());

         uni.onItem().transformToMulti(c -> c.query("where a > 5").param("","").find());

         uni.onItem().invoke(c -> c.put("k", "v")).subscribe();
      }
   }
}
