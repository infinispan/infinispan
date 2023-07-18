package org.infinispan.server.resp;

import io.lettuce.core.KeyValue;
import io.lettuce.core.LMoveArgs;
import io.lettuce.core.LPosArgs;
import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.api.sync.RedisCommands;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;

import static io.lettuce.core.LMPopArgs.Builder.left;
import static io.lettuce.core.LMPopArgs.Builder.right;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.infinispan.server.resp.test.RespTestingUtil.assertWrongType;

/**
 * RESP List commands testing
 *
 * @since 15.0
 */
@Test(groups = "functional", testName = "server.resp.RespListCommandsTest")
public class RespListCommandsTest extends SingleNodeRespBaseTest {

   RedisCommands<String, String> redis;

   @BeforeMethod
   public void initConnection() {
      redis = redisConnection.sync();
   }

   public void testRPUSH() {
      long result = redis.rpush("people", "tristan");
      assertThat(result).isEqualTo(1);

      result = redis.rpush("people", "william");
      assertThat(result).isEqualTo(2);

      result = redis.rpush("people", "william", "jose", "pedro");
      assertThat(result).isEqualTo(5);
      assertThat(redis.lrange("people", 0, 4)).containsExactly("tristan", "william", "william", "jose", "pedro");

      assertWrongType(() -> redis.set("leads", "tristan"), () ->  redis.rpush("leads", "william"));
   }

   public void testRPUSHX() {
      long result = redis.rpushx("noexisting", "doraemon", "son goku");
      assertThat(result).isEqualTo(0);

      result = redis.rpush("existing", "tristan");
      assertThat(result).isEqualTo(1);

      result = redis.rpushx("existing", "william", "jose");
      assertThat(result).isEqualTo(3);
      assertThat(redis.lrange("existing", 0, 2)).containsExactly("tristan", "william", "jose");

      assertWrongType(() -> redis.set("leads", "tristan"), () -> redis.rpushx("leads", "william"));
   }

   public void testLPUSH() {
      long result = redis.lpush("people", "tristan");
      assertThat(result).isEqualTo(1);

      result = redis.lpush("people", "william");
      assertThat(result).isEqualTo(2);

      result = redis.lpush("people", "william", "jose", "pedro");
      assertThat(result).isEqualTo(5);
      assertThat(redis.lrange("people", 0, 4)).containsExactly("pedro", "jose", "william", "william", "tristan");

      assertWrongType(() -> redis.set("leads", "tristan"), () -> redis.lpush("leads", "william"));
   }

   public void testLPUSHX() {
      long result = redis.lpushx("noexisting", "doraemon", "son goku");
      assertThat(result).isEqualTo(0);

      result = redis.lpush("existing", "tristan");
      assertThat(result).isEqualTo(1);

      result = redis.lpushx("existing", "william", "jose");
      assertThat(result).isEqualTo(3);
      assertThat(redis.lrange("existing", 0, 2)).containsExactly("jose", "william", "tristan");

      assertWrongType(() -> redis.set("leads", "tristan"), () -> redis.lpushx("leads", "william"));
   }

   public void testRPOP() {
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

      assertWrongType(() -> redis.set("leads", "tristan"), () ->  redis.rpop("leads"));

      // RPOP the count argument is negative
      assertThatThrownBy(() -> {
         redis.rpop("leads", -42);
      }).isInstanceOf(RedisCommandExecutionException.class)
            .hasMessageContaining("ERR value is out of range, must be positive");
   }

   public void testLPOP() {
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

      assertWrongType(() -> redis.set("leads", "tristan"), () -> redis.lpop("leads"));

      // RPOP the count argument is negative
      assertThatThrownBy(() -> {
         redis.lpop("leads", -42);
      }).isInstanceOf(RedisCommandExecutionException.class)
            .hasMessageContaining("ERR value is out of range, must be positive");
   }

   public void testLINDEX() {
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

      assertWrongType(() -> redis.set("another", "tristan"), () -> redis.lindex("another", 1));
   }

   public void testLLEN() {
      assertThat(redis.llen("noexisting")).isEqualTo(0);

      redis.rpush("leads", "william", "jose", "ryan", "pedro", "vittorio");
      assertThat(redis.llen("leads")).isEqualTo(5);

      assertWrongType(() -> redis.set("another", "tristan"), () -> redis.llen("another"));
   }

   public void testLRANGE() {
      assertThat(redis.lrange("noexisting", -1, 3)).isEmpty();

      redis.rpush("leads", "william", "jose", "ryan", "pedro", "vittorio");
      assertThat(redis.lrange("leads", 0, 5)).containsExactly("william", "jose", "ryan", "pedro", "vittorio");
      assertThat(redis.lrange("leads", 1, -1)).containsExactly("jose", "ryan", "pedro", "vittorio");
      assertThat(redis.lrange("leads", 3, 3)).containsExactly("pedro");

      assertWrongType(() -> redis.set("another", "tristan"), () -> redis.llen("another"));
   }

   public void testLSET() {
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

      assertWrongType(() -> redis.set("another", "tristan"), () -> redis.lset("another", 0, "tristan"));
   }

   public void testLPOS() {
      redis.rpush("leads", "william", "jose", "ryan", "pedro", "vittorio", "ryan", "michael", "ryan");

      assertThat(redis.lpos("not_existing", "ryan")).isNull();
      assertThat(redis.lpos("leads", "ramona")).isNull();
      assertThat(redis.lpos("leads", "ryan")).isEqualTo(2);
      assertThat(redis.lpos("leads", "ryan", LPosArgs.Builder.rank(1))).isEqualTo(2);
      assertThat(redis.lpos("leads", "ryan", LPosArgs.Builder.rank(-1))).isEqualTo(7);
      assertThat(redis.lpos("leads", "ryan", LPosArgs.Builder.rank(-2))).isEqualTo(5);
      assertThat(redis.lpos("leads", "ryan", LPosArgs.Builder.rank(2))).isEqualTo(5);
      assertThat(redis.lpos("leads", "ryan", LPosArgs.Builder.maxlen(3))).isEqualTo(2);
      assertThat(redis.lpos("leads", "ryan", LPosArgs.Builder.maxlen(2))).isNull();

      assertThat(redis.lpos("leads", "ryan", 0)).containsExactly(2L, 5L, 7L);
      assertThat(redis.lpos("leads", "ryan", 1)).containsExactly(2L);
      assertThat(redis.lpos("leads", "ryan", 2)).containsExactly(2L, 5L);
      assertThat(redis.lpos("leads", "ryan", 3)).containsExactly(2L, 5L, 7L);
      assertThat(redis.lpos("leads", "ryan", 10)).containsExactly(2L, 5L, 7L);
      assertThat(redis.lpos("leads", "ryan", 0, LPosArgs.Builder.rank(2))).containsExactly(5L, 7L);
      assertThat(redis.lpos("leads", "ryan", 2, LPosArgs.Builder.rank(-2))).containsExactly(5L, 2L);
      assertThat(redis.lpos("leads", "ryan", 2, LPosArgs.Builder.rank(1))).containsExactly(2L, 5L);
      assertThat(redis.lpos("leads", "ramona", 0)).isEmpty();

      // LPOS on an existing key that contains a String, not a Collection!
      assertThatThrownBy(() -> {
         redis.lpos("leads", "ryan", LPosArgs.Builder.rank(0));
      }).isInstanceOf(RedisCommandExecutionException.class)
            .hasMessageContaining("ERR RANK can't be zero");

      assertWrongType(() -> redis.set("another", "tristan"), () -> redis.lpos("another","tristan"));
   }

   public void testLINSERT() {
      redis.rpush("leads", "william", "jose", "ryan", "pedro", "jose");
      assertThat(redis.linsert("not_exsiting", true, "william", "fabio")).isEqualTo(0);
      assertThat(redis.linsert("leads", true, "vittorio", "fabio")).isEqualTo(-1);
      assertThat(redis.linsert("leads", true, "jose", "fabio")).isEqualTo(6);
      assertThat(redis.lrange("leads",0, -1)).containsExactly("william", "fabio", "jose", "ryan", "pedro", "jose");
      assertThat(redis.linsert("leads", false, "jose", "fabio")).isEqualTo(7);
      assertThat(redis.lrange("leads",0, -1)).containsExactly("william", "fabio", "jose", "fabio", "ryan", "pedro", "jose");

      assertWrongType(() -> redis.set("another", "tristan"), () -> redis.linsert("another", true,"tristan", "william"));
   }

   public void testLREM() {
      redis.rpush("leads", "william", "jose", "ryan", "pedro", "jose", "pedro", "pedro", "tristan", "pedro");
      assertThat(redis.lrem("not_existing", 1, "ramona")).isEqualTo(0);
      assertThat(redis.lrem("leads", 1, "ramona")).isEqualTo(0);
      assertThat(redis.lrem("leads", 1, "pedro")).isEqualTo(1);
      assertThat(redis.lrange("leads", 0, -1)).containsExactly("william", "jose", "ryan", "jose", "pedro", "pedro", "tristan", "pedro");
      assertThat(redis.lrem("leads", -2, "pedro")).isEqualTo(2);
      assertThat(redis.lrange("leads", 0, -1)).containsExactly("william", "jose", "ryan", "jose", "pedro", "tristan");
      assertThat(redis.lrem("leads", 0, "jose")).isEqualTo(2);
      assertThat(redis.lrange("leads", 0, -1)).containsExactly("william", "ryan", "pedro", "tristan");
      redis.lrem("leads", 0, "tristan");
      redis.lrem("leads", 0, "william");
      redis.lrem("leads", 0, "pedro");
      assertThat(redis.exists("leads")).isEqualTo(1);
      redis.lrem("leads", 0, "ryan");
      assertThat(redis.exists("leads")).isEqualTo(0);

      assertWrongType(() -> redis.set("another", "tristan"), () -> redis.lrem("another", 0,"tristan"));
   }

   public void testLTRIM() {
      assertThat(redis.lrange("noexisting", -1, 3)).isEmpty();

      redis.rpush("leads", "william", "jose", "ryan", "pedro", "vittorio");
      assertThat(redis.ltrim("leads", 0, 2)).isEqualTo("OK");
      assertThat(redis.lrange("leads", 0, -1)).containsExactly("william", "jose", "ryan");
      assertThat(redis.ltrim("leads", 1, 1)).isEqualTo("OK");
      assertThat(redis.lrange("leads", 0, -1)).containsExactly("jose");
      assertThat(redis.ltrim("leads", 1, -1)).isEqualTo("OK");
      assertThat(redis.exists("leads")).isEqualTo(0);

      assertWrongType(() -> redis.set("another", "tristan"), () -> redis.ltrim("another", 0, 2));
   }

   public void testLMOVE() {
      redis.rpush("leads", "william", "tristan", "pedro", "jose", "ryan");
      assertThat(redis.lmove("not_existing", "leads", LMoveArgs.Builder.rightRight())).isNull();

      // Rotate
      assertThat(redis.lmove("leads", "leads", LMoveArgs.Builder.rightRight())).isEqualTo("ryan");
      assertThat(redis.lrange("leads", 0, -1)).containsExactly("william", "tristan", "pedro", "jose", "ryan");
      assertThat(redis.lmove("leads", "leads", LMoveArgs.Builder.leftLeft())).isEqualTo("william");
      assertThat(redis.lrange("leads", 0, -1)).containsExactly("william", "tristan", "pedro", "jose", "ryan");
      assertThat(redis.lmove("leads", "leads", LMoveArgs.Builder.leftRight())).isEqualTo("william");
      assertThat(redis.lrange("leads", 0, -1)).containsExactly("tristan", "pedro", "jose", "ryan", "william");
      assertThat(redis.lmove("leads", "leads", LMoveArgs.Builder.rightLeft())).isEqualTo("william");
      assertThat(redis.lrange("leads", 0, -1)).containsExactly("william", "tristan", "pedro", "jose", "ryan");

      // Move from two lists
      redis.rpush("fantasy_leads", "doraemon", "son goku", "snape");
      assertThat(redis.lmove("leads", "fantasy_leads", LMoveArgs.Builder.rightRight())).isEqualTo("ryan");
      assertThat(redis.lrange("leads", 0, -1)).containsExactly("william", "tristan", "pedro", "jose");
      assertThat(redis.lrange("fantasy_leads", 0, -1)).containsExactly("doraemon", "son goku", "snape", "ryan");

      assertThat(redis.lmove("leads", "fantasy_leads", LMoveArgs.Builder.rightLeft())).isEqualTo("jose");
      assertThat(redis.lrange("leads", 0, -1)).containsExactly("william", "tristan", "pedro");
      assertThat(redis.lrange("fantasy_leads", 0, -1)).containsExactly("jose", "doraemon", "son goku", "snape", "ryan");

      assertThat(redis.lmove("leads", "fantasy_leads", LMoveArgs.Builder.leftRight())).isEqualTo("william");
      assertThat(redis.lrange("leads", 0, -1)).containsExactly("tristan", "pedro");
      assertThat(redis.lrange("fantasy_leads", 0, -1)).containsExactly("jose", "doraemon", "son goku", "snape", "ryan", "william");

      assertThat(redis.lmove("leads", "fantasy_leads", LMoveArgs.Builder.leftLeft())).isEqualTo("tristan");
      assertThat(redis.lrange("leads", 0, -1)).containsExactly("pedro");
      assertThat(redis.lrange("fantasy_leads", 0, -1)).containsExactly("tristan", "jose", "doraemon", "son goku", "snape", "ryan", "william");

      assertThat(redis.lmove("leads", "new_leads", LMoveArgs.Builder.leftLeft())).isEqualTo("pedro");
      assertThat(redis.lrange("leads", 0, -1)).isEmpty();
      assertThat(redis.lrange("new_leads", 0, -1)).containsExactly("pedro");

      assertWrongType(() -> redis.set("another", "tristan"), () -> redis.lmove("another", "another", LMoveArgs.Builder.leftRight()));
   }

   public void testRPOPLPUSH() {
      redis.rpush("leads", "william", "tristan", "pedro", "jose", "ryan");
      assertThat(redis.rpoplpush("not_existing", "leads")).isNull();

      // Rotate
      assertThat(redis.lrange("leads", 0, -1)).containsExactly("william", "tristan", "pedro", "jose", "ryan");
      assertThat(redis.rpoplpush("leads", "leads")).isEqualTo("ryan");
      assertThat(redis.lrange("leads", 0, -1)).containsExactly("ryan", "william", "tristan", "pedro", "jose");

      // Move from two lists
      redis.rpush("fantasy_leads", "doraemon", "son goku", "snape");

      assertThat(redis.rpoplpush("leads", "fantasy_leads")).isEqualTo("jose");
      assertThat(redis.lrange("leads", 0, -1)).containsExactly("ryan", "william", "tristan", "pedro");
      assertThat(redis.lrange("fantasy_leads", 0, -1)).containsExactly("jose", "doraemon", "son goku", "snape");

      assertThat(redis.rpoplpush("leads", "new_leads")).isEqualTo("pedro");
      assertThat(redis.lrange("leads", 0, -1)).containsExactly("ryan", "william", "tristan");
      assertThat(redis.lrange("new_leads", 0, -1)).containsExactly("pedro");

      assertWrongType(() -> redis.set("another", "tristan"), () -> redis.rpoplpush("another", "another"));
   }

   public void testLMPOP() {
      assertThat(redis.lmpop(left(), "unk1")).isNull();
      assertThat(redis.lmpop(right(), "unk1")).isNull();
      assertThat(redis.lmpop(left(), "unk1", "unk2", "unk3")).isNull();
      assertThat(redis.lmpop(right(), "unk1", "unk2", "unk3")).isNull();

      redis.rpush("leads", "william", "jose", "ryan", "pedro", "vittorio");
      KeyValue<String, List<String>> call = redis.lmpop(right(), "unk1", "unk2", "leads");
      assertThat(call.getKey()).isEqualTo("leads");
      assertThat(call.getValue()).containsExactly("vittorio");

      call = redis.lmpop(left().count(2), "unk1", "leads", "unk2");
      assertThat(call.getKey()).isEqualTo("leads");
      assertThat(call.getValue()).containsExactly("william", "jose");

      call = redis.lmpop(left().count(4), "leads", "unk1", "unk2");
      assertThat(call.getKey()).isEqualTo("leads");
      assertThat(call.getValue()).containsExactly("ryan", "pedro");

      assertThat(redis.exists("leads")).isEqualTo(0);
      assertWrongType(() -> redis.set("another", "tristan"), () -> redis.lmpop(left(), "another"));
   }
}
