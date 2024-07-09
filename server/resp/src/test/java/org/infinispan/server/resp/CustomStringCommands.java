package org.infinispan.server.resp;

import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.dynamic.Commands;
import io.lettuce.core.dynamic.RedisCommandFactory;
import io.lettuce.core.dynamic.annotation.Command;
import io.lettuce.core.dynamic.annotation.Param;

public interface CustomStringCommands extends Commands {

   @Command("LCS :k1 :k2")
   byte[] lcs(@Param("k1") byte[] k1, @Param("k2") byte[] k2);

   @Command("LCS :k1 :k2 LEN")
   Long lcsLen(@Param("k1") byte[] k1, @Param("k2") byte[] k2);

   @Command("MSETNX :k1 :v1 :k1 :v2 :k1 :v3 :k1 :v4")
   Long msetnxSameKey(@Param("k1") byte[] k1, @Param("v1") byte[] v1,
                             @Param("v2") byte[] v2, @Param("v3") byte[] v3,
                             @Param("v4") byte[] v4);

   static CustomStringCommands instance(StatefulConnection<String, String> conn) {
      RedisCommandFactory factory = new RedisCommandFactory(conn);
      return factory.getCommands(CustomStringCommands.class);
   }
}
