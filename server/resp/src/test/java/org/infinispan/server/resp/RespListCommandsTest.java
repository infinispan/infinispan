package org.infinispan.server.resp;

import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.api.sync.RedisCommands;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * RESP List commands testing
 *
 * @since 15.0
 */
@Test(groups = "functional", testName = "server.resp.RespListCommandsTest")
public class RespListCommandsTest extends SingleNodeRespBaseTest {

   public void testRPUSH() {
      RedisCommands<String, String> redis = redisConnection.sync();
      long result = redis.rpush("people", "tristan");
      assertThat(result).isEqualTo(1);

      result = redis.rpush("people", "william");
      assertThat(result).isEqualTo(2);

      result = redis.rpush("people", "william", "jose", "pedro");
      assertThat(result).isEqualTo(5);
      assertThat(redis.lrange("people", 0, 4)).containsExactly("tristan", "william", "william", "jose", "pedro");

      // Set a String Command
      redis.set("leads", "tristan");

      // Push on an existing key that contains a String, not a Collection!
      assertThatThrownBy(() -> {
               redis.rpush("leads", "william");
      }).isInstanceOf(RedisCommandExecutionException.class)
            .hasMessageContaining("ERRWRONGTYPE");
   }

   public void testRPUSHX() {
      RedisCommands<String, String> redis = redisConnection.sync();
      long result = redis.rpushx("noexisting", "doraemon", "son goku");
      assertThat(result).isEqualTo(0);

      result = redis.rpush("existing", "tristan");
      assertThat(result).isEqualTo(1);

      result = redis.rpushx("existing", "william", "jose");
      assertThat(result).isEqualTo(3);
      assertThat(redis.lrange("existing", 0, 2)).containsExactly("tristan", "william", "jose");
      // Set a String Command
      redis.set("leads", "tristan");

      // RPUSHX on an existing key that contains a String, not a Collection!
      assertThatThrownBy(() -> {
         redis.rpushx("leads", "william");
      }).isInstanceOf(RedisCommandExecutionException.class)
            .hasMessageContaining("ERRWRONGTYPE");
   }

   public void testLPUSH() {
      RedisCommands<String, String> redis = redisConnection.sync();
      long result = redis.lpush("people", "tristan");
      assertThat(result).isEqualTo(1);

      result = redis.lpush("people", "william");
      assertThat(result).isEqualTo(2);

      result = redis.lpush("people", "william", "jose", "pedro");
      assertThat(result).isEqualTo(5);
      assertThat(redis.lrange("people", 0, 4)).containsExactly("pedro", "jose", "william", "william", "tristan");

      // Set a String Command
      redis.set("leads", "tristan");

      // Push on an existing key that contains a String, not a Collection!
      assertThatThrownBy(() -> {
         redis.lpush("leads", "william");
      }).isInstanceOf(RedisCommandExecutionException.class)
            .hasMessageContaining("ERRWRONGTYPE");
   }

   public void testLPUSHX() {
      RedisCommands<String, String> redis = redisConnection.sync();
      long result = redis.lpushx("noexisting", "doraemon", "son goku");
      assertThat(result).isEqualTo(0);

      result = redis.lpush("existing", "tristan");
      assertThat(result).isEqualTo(1);

      result = redis.lpushx("existing", "william", "jose");
      assertThat(result).isEqualTo(3);
      assertThat(redis.lrange("existing", 0, 2)).containsExactly("jose", "william", "tristan");

      // Set a String Command
      redis.set("leads", "tristan");

      // LPUSHX on an existing key that contains a String, not a Collection!
      assertThatThrownBy(() -> {
         redis.lpushx("leads", "william");
      }).isInstanceOf(RedisCommandExecutionException.class)
            .hasMessageContaining("ERRWRONGTYPE");
   }

   public void testRPOP() {
      RedisCommands<String, String> redis = redisConnection.sync();
      assertThat(redis.rpop("not_exist")).isNull();

      // test single value
      assertThat(redis.rpush("leads", "tristan")).isEqualTo(1);
      assertThat(redis.rpop("leads")).isEqualTo("tristan");
      assertThat(redis.rpop("leads")).isNull();

      // test multiple values
      assertThat(redis.rpush("leads", "tristan", "jose", "william", "pedro")).isEqualTo(4);
      assertThat(redis.rpop("leads", 0)).isEmpty();
      assertThat(redis.rpop("leads", 3)).containsExactly( "pedro", "william", "jose");
      assertThat(redis.rpop("leads", 1)).containsExactly("tristan");
      assertThat(redis.rpop("leads")).isNull();

      //Set a String Command
      redis.set("leads", "tristan");

      //RPOP on an existing key that contains a String, not a Collection!
      assertThatThrownBy(() -> {
         redis.rpop("leads");
      }).isInstanceOf(RedisCommandExecutionException.class)
            .hasMessageContaining("ERRWRONGTYPE");

      // RPOP the count argument is negative
      assertThatThrownBy(() -> {
         redis.rpop("leads", -42);
      }).isInstanceOf(RedisCommandExecutionException.class)
            .hasMessageContaining("ERR value is out of range, must be positive");
   }

   public void testLPOP() {
      RedisCommands<String, String> redis = redisConnection.sync();
      assertThat(redis.lpop("not_exist")).isNull();

      // test single value
      assertThat(redis.rpush("leads", "tristan")).isEqualTo(1);
      assertThat(redis.lpop("leads")).isEqualTo("tristan");
      assertThat(redis.lpop("leads")).isNull();

      // test multiple values
      assertThat(redis.rpush("leads", "tristan", "jose", "william", "pedro")).isEqualTo(4);
      assertThat(redis.lpop("leads", 0)).isEmpty();
      assertThat(redis.lpop("leads", 3)).containsExactly(  "tristan", "jose", "william");
      assertThat(redis.lpop("leads", 1)).containsExactly("pedro");
      assertThat(redis.lpop("leads")).isNull();

      //Set a String Command
      redis.set("leads", "tristan");

      //RPOP on an existing key that contains a String, not a Collection!
      assertThatThrownBy(() -> {
         redis.lpop("leads");
      }).isInstanceOf(RedisCommandExecutionException.class)
            .hasMessageContaining("ERRWRONGTYPE");

      // RPOP the count argument is negative
      assertThatThrownBy(() -> {
         redis.lpop("leads", -42);
      }).isInstanceOf(RedisCommandExecutionException.class)
            .hasMessageContaining("ERR value is out of range, must be positive");
   }

   public void testLINDEX() {
      RedisCommands<String, String> redis = redisConnection.sync();
      assertThat(redis.lindex("noexisting", 10)).isNull();

      redis.rpush("leads", "tristan");
      assertThat(redis.lindex("leads", 0)).isEqualTo("tristan");
      assertThat(redis.lindex("leads", -1)).isEqualTo("tristan");
      assertThat(redis.lindex("leads", 1)).isNull();
      assertThat(redis.lindex("leads", -2)).isNull();

      redis.rpush("leads", "william", "jose", "ryan", "pedro", "vittorio");
      // size 6: ["tristan", "william", "jose", "ryan", "pedro", "vittorio"]
      assertThat(redis.lindex("leads", 1)).isEqualTo("william");
      assertThat(redis.lindex("leads", -1)).isEqualTo("vittorio");
      assertThat(redis.lindex("leads", -6)).isEqualTo("tristan");
      assertThat(redis.lindex("leads", 3)).isEqualTo("ryan");
      assertThat(redis.lindex("leads", -3)).isEqualTo("ryan");
      assertThat(redis.lindex("leads", 6)).isNull();
      assertThat(redis.lindex("leads", -7)).isNull();

      // Set a String Command
      redis.set("another", "tristan");

      // LINDEX on an existing key that contains a String, not a Collection!
      assertThatThrownBy(() -> {
         redis.lindex("another", 1);
      }).isInstanceOf(RedisCommandExecutionException.class)
            .hasMessageContaining("ERRWRONGTYPE");
   }

   public void testLLEN() {
      RedisCommands<String, String> redis = redisConnection.sync();
      assertThat(redis.llen("noexisting")).isEqualTo(0);

      redis.rpush("leads", "william", "jose", "ryan", "pedro", "vittorio");
      assertThat(redis.llen("leads")).isEqualTo(5);

      // Set a String Command
      redis.set("another", "tristan");

      // LLEN on an existing key that contains a String, not a Collection!
      assertThatThrownBy(() -> {
         redis.llen("another");
      }).isInstanceOf(RedisCommandExecutionException.class)
            .hasMessageContaining("ERRWRONGTYPE");
   }

   public void testLRANGE() {
      RedisCommands<String, String> redis = redisConnection.sync();
      assertThat(redis.lrange("noexisting", -1, 3)).isEmpty();

      redis.rpush("leads", "william", "jose", "ryan", "pedro", "vittorio");
      assertThat(redis.lrange("leads", 0, 5)).containsExactly("william", "jose", "ryan", "pedro", "vittorio");
      assertThat(redis.lrange("leads", 1, -1)).containsExactly("jose", "ryan", "pedro", "vittorio");
      assertThat(redis.lrange("leads", 3, 3)).containsExactly("pedro");

      // Set a String Command
      redis.set("another", "tristan");

      // LLEN on an existing key that contains a String, not a Collection!
      assertThatThrownBy(() -> {
         redis.llen("another");
      }).isInstanceOf(RedisCommandExecutionException.class)
            .hasMessageContaining("ERRWRONGTYPE");
   }

   public void testLSET() {
      RedisCommands<String, String> redis = redisConnection.sync();
      redis.rpush("leads", "william", "jose", "ryan", "pedro", "vittorio");

      assertThat(redis.lset("leads", 0,  "fabio")).isEqualTo("OK");
      assertThat(redis.lindex("leads", 0)).isEqualTo("fabio");
      assertThat(redis.lset("leads", -1,  "tristan")).isEqualTo("OK");
      assertThat(redis.lindex("leads", -1)).isEqualTo("tristan");

      assertThat(redis.lset("leads", 2,  "wolf")).isEqualTo("OK");
      assertThat(redis.lindex("leads", 2)).isEqualTo("wolf");

      assertThat(redis.lset("leads", -3,  "anna")).isEqualTo("OK");
      assertThat(redis.lindex("leads", -3)).isEqualTo("anna");

      assertThatThrownBy(() -> {
         redis.lset("leads", 5, "dan");
      }).isInstanceOf(RedisCommandExecutionException.class)
            .hasMessageContaining("ERR index out of range");

      assertThatThrownBy(() -> {
         redis.lset("leads", -6, "dan");
      }).isInstanceOf(RedisCommandExecutionException.class)
            .hasMessageContaining("ERR index out of range");

      assertThatThrownBy(() -> {
         redis.lset("not_existing", 0, "tristan");
      }).isInstanceOf(RedisCommandExecutionException.class)
            .hasMessageContaining("ERR no such key");

      // Set a String Command
      redis.set("another", "tristan");

      // LSET on an existing key that contains a String, not a Collection!
      assertThatThrownBy(() -> {
         redis.lset("another", 0, "tristan");
      }).isInstanceOf(RedisCommandExecutionException.class)
            .hasMessageContaining("ERRWRONGTYPE");
   }
}
