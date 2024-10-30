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

   @Command("LPOP :k1 :k2 :k3")
   Long lpopWrongArgNum(@Param("k1") byte[] k1, @Param("k2") byte[] k2, @Param("k3") byte[] k3);

   @Command("RPOP :k1 :k2 :k3")
   Long rpopWrongArgNum(@Param("k1") byte[] k1, @Param("k2") byte[] k2, @Param("k3") byte[] k3);

   @Command("SINTERCARD :k1 :k2 :k3 :k4 :k5")
   Long sintercard5Args(@Param("k1") byte[] k1, @Param("k2") byte[] k2,
                             @Param("k3") byte[] k3, @Param("k4") byte[] k4,
                             @Param("k5") byte[] k5);

   @Command("LPOS :k1 :k2 RANK :k3")
   Long lposRank(@Param("k1") byte[] k1, @Param("k2") byte[] k2, @Param("k3") byte[] k3);


   @Command("JSON.SET :key :path :value")
   String jsonCmd(@Param("key") String key, @Param("path") String path, @Param("value") String value);

   @Command("JSON.SET :key :path :value :arg")
   String jsonCmdWithArg(@Param("cmd") String cmd, @Param("key") String key, @Param("path") String path, @Param("value") String value, @Param("arg") String arg);

   static CustomStringCommands instance(StatefulConnection<String, String> conn) {
      RedisCommandFactory factory = new RedisCommandFactory(conn);
      return factory.getCommands(CustomStringCommands.class);
   }
}
