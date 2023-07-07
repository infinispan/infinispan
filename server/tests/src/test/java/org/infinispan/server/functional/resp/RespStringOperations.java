package org.infinispan.server.functional.resp;

import org.infinispan.server.functional.ClusteredIT;
import org.infinispan.server.test.junit5.InfinispanServerExtension;
import org.junit.jupiter.api.extension.RegisterExtension;

import io.vertx.redis.client.Redis;

public class RespStringOperations {

   @RegisterExtension
   public static InfinispanServerExtension SERVERS = ClusteredIT.SERVERS;


   private Redis createClient() {
      return SERVERS.resp().get();
   }
}
