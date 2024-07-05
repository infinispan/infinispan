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

   static CustomStringCommands instance(StatefulConnection<String, String> conn) {
      RedisCommandFactory factory = new RedisCommandFactory(conn);
      return factory.getCommands(CustomStringCommands.class);
   }
}
