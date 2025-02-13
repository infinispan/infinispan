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
   String jsonSet(@Param("key") String key, @Param("path") String path, @Param("value") String value);

   @Command("JSON.GET :key")
   String jsonGet(@Param("key") String key);

   @Command("JSON.GET :key :path")
   String jsonGet(@Param("key") String key, @Param("path") String path);

   @Command("JSON.GET :key :path1 :path2")
   String jsonGet(@Param("key") String key, @Param("path") String path1, @Param("path") String path2);

   @Command("JSON.GET :key :path1 :path2 :path3")
   String jsonGet(@Param("key") String key, @Param("path") String path1, @Param("path") String path2, @Param("path") String path3);

   // Lettuce arrAppend command has a bug, it doesn't support root path, so we need to use a custom command
   @Command("JSON.ARRAPPEND :key :path :v1 :v2 :v3")
   Long jsonArrappend(@Param("key") String key, @Param("path") String path, @Param("v1") String v1, @Param("v2") String v2, @Param("v2") String v3);

   static CustomStringCommands instance(StatefulConnection<String, String> conn) {
      RedisCommandFactory factory = new RedisCommandFactory(conn);
      return factory.getCommands(CustomStringCommands.class);
   }
}
