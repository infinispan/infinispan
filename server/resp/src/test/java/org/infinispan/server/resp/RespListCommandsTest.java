package org.infinispan.server.resp;

import static io.lettuce.core.LMPopArgs.Builder.left;
import static io.lettuce.core.LMPopArgs.Builder.right;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.infinispan.server.resp.test.RespTestingUtil.assertWrongType;

import java.util.List;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import io.lettuce.core.KeyValue;
import io.lettuce.core.LMoveArgs;
import io.lettuce.core.LPosArgs;
import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.output.IntegerOutput;
import io.lettuce.core.protocol.CommandArgs;
import io.lettuce.core.protocol.CommandType;

/**
 * RESP List commands testing
 *
 * @since 15.0
 */
@Test(groups = "functional", testName = "server.resp.RespListCommandsTest")
public class RespListCommandsTest extends SingleNodeRespBaseTest {

   RedisCommands<String, String> redis;

   @Override
   public Object[] factory() {
      return new Object[] {
            new RespListCommandsTest(),
            new RespListCommandsTest().withAuthorization(),
      };
   }

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

      // Test that order is correct with new list
      result = redis.rpush("people2", "william", "jose", "pedro");
      assertThat(result).isEqualTo(3);
      assertThat(redis.lrange("people2", 0, -1)).containsExactly("william", "jose", "pedro");

      assertWrongType(() -> redis.set("leads", "tristan"), () -> redis.rpush("leads", "william"));
      assertWrongType(() -> redis.rpush("data", "e1"), () -> redis.get("data"));
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

      // Test that order is correct with new list
      result = redis.lpush("people2", "william", "jose", "pedro");
      assertThat(result).isEqualTo(3);
      assertThat(redis.lrange("people2", 0, -1)).containsExactly("pedro", "jose", "william");

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
      assertThat(redis.rpop("leads", 3)).containsExactly("pedro", "william", "jose");
      assertThat(redis.rpop("leads", 1)).containsExactly("tristan");
      assertThat(redis.rpop("leads")).isNull();

      assertWrongType(() -> redis.set("leads", "tristan"), () -> redis.rpop("leads"));

      // RPOP the count argument is negative
      assertThatThrownBy(() -> {
         redis.rpop("leads", -42);
      }).isInstanceOf(RedisCommandExecutionException.class)
            .hasMessageContaining("ERR value is out of range, must be positive");

      CustomStringCommands commands = CustomStringCommands.instance(redisConnection);
      assertThatThrownBy(() -> {
         commands.rpopWrongArgNum("leads".getBytes(), "1".getBytes(), "2".getBytes());
         }).isInstanceOf(RedisCommandExecutionException.class)
         .hasMessageContaining("ERR wrong number of arguments for 'rpop' command");
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
      assertThat(redis.lpop("leads", 3)).containsExactly("tristan", "jose", "william");
      assertThat(redis.lpop("leads", 1)).containsExactly("pedro");
      assertThat(redis.lpop("leads")).isNull();

      assertWrongType(() -> redis.set("leads", "tristan"), () -> redis.lpop("leads"));

      // RPOP the count argument is negative
      assertThatThrownBy(() -> {
         redis.lpop("leads", -42);
         }).isInstanceOf(RedisCommandExecutionException.class)
            .hasMessageContaining("ERR value is out of range, must be positive");

      CustomStringCommands commands = CustomStringCommands.instance(redisConnection);
      assertThatThrownBy(() -> {
         commands.lpopWrongArgNum("leads".getBytes(), "1".getBytes(), "2".getBytes());
         }).isInstanceOf(RedisCommandExecutionException.class)
         .hasMessageContaining("ERR wrong number of arguments for 'lpop' command");
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

      assertThat(redis.lset("leads", 0, "fabio")).isEqualTo("OK");
      assertThat(redis.lindex("leads", 0)).isEqualTo("fabio");
      assertThat(redis.lset("leads", -1, "tristan")).isEqualTo("OK");
      assertThat(redis.lindex("leads", -1)).isEqualTo("tristan");

      assertThat(redis.lset("leads", 2, "wolf")).isEqualTo("OK");
      assertThat(redis.lindex("leads", 2)).isEqualTo("wolf");

      assertThat(redis.lset("leads", -3, "anna")).isEqualTo("OK");
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

      // LPOS with RANK=0 throws exception
      assertThatThrownBy(() -> {
         redis.lpos("leads", "ryan", LPosArgs.Builder.rank(0));
      }).isInstanceOf(RedisCommandExecutionException.class)
            .hasMessageContaining("ERR RANK can't be zero");

      // LPOS with RANK=-9223372036854775808 throws exception with ad hoc message
      assertThatThrownBy(() -> {
         redis.lpos("leads", "ryan", LPosArgs.Builder.rank(-9223372036854775808L));
      }).isInstanceOf(RedisCommandExecutionException.class)
            .hasMessageContaining("ERR value is out of range, value must between"
                  +" -9223372036854775807 and 9223372036854775807");

      // LPOS with RANK value out of Long range
      CustomStringCommands commands = CustomStringCommands.instance(redisConnection);
      assertThatThrownBy(() -> {
         commands.lposRank("leads".getBytes(), "1".getBytes(), "92233720368547758081".getBytes());
      }).isInstanceOf(RedisCommandExecutionException.class)
      .hasMessageContaining("ERR value is not an integer or out of range");
      // LPOS on an existing key that contains a String, not a Collection!
      assertWrongType(() -> redis.set("another", "tristan"), () -> redis.lpos("another", "tristan"));
   }

   public void testLINSERT() {
      redis.rpush("leads", "william", "jose", "ryan", "pedro", "jose");
      assertThat(redis.linsert("not_exsiting", true, "william", "fabio")).isEqualTo(0);
      assertThat(redis.linsert("leads", true, "vittorio", "fabio")).isEqualTo(-1);
      assertThat(redis.linsert("leads", true, "jose", "fabio")).isEqualTo(6);
      assertThat(redis.lrange("leads", 0, -1)).containsExactly("william", "fabio", "jose", "ryan", "pedro", "jose");
      assertThat(redis.linsert("leads", false, "jose", "fabio")).isEqualTo(7);
      assertThat(redis.lrange("leads", 0, -1)).containsExactly("william", "fabio", "jose", "fabio", "ryan", "pedro",
            "jose");

      assertWrongType(() -> redis.set("another", "tristan"),
            () -> redis.linsert("another", true, "tristan", "william"));
   }

   public void testLREM() {
      redis.rpush("leads", "william", "jose", "ryan", "pedro", "jose", "pedro", "pedro", "tristan", "pedro");
      assertThat(redis.lrem("not_existing", 1, "ramona")).isEqualTo(0);
      assertThat(redis.lrem("leads", 1, "ramona")).isEqualTo(0);
      assertThat(redis.lrem("leads", 1, "pedro")).isEqualTo(1);
      assertThat(redis.lrange("leads", 0, -1)).containsExactly("william", "jose", "ryan", "jose", "pedro", "pedro",
            "tristan", "pedro");
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

      assertWrongType(() -> redis.set("another", "tristan"), () -> redis.lrem("another", 0, "tristan"));
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
      assertThat(redis.lrange("fantasy_leads", 0, -1)).containsExactly("jose", "doraemon", "son goku", "snape", "ryan",
            "william");

      assertThat(redis.lmove("leads", "fantasy_leads", LMoveArgs.Builder.leftLeft())).isEqualTo("tristan");
      assertThat(redis.lrange("leads", 0, -1)).containsExactly("pedro");
      assertThat(redis.lrange("fantasy_leads", 0, -1)).containsExactly("tristan", "jose", "doraemon", "son goku",
            "snape", "ryan", "william");

      assertThat(redis.lmove("leads", "new_leads", LMoveArgs.Builder.leftLeft())).isEqualTo("pedro");
      assertThat(redis.lrange("leads", 0, -1)).isEmpty();
      assertThat(redis.lrange("new_leads", 0, -1)).containsExactly("pedro");

      assertWrongType(() -> redis.set("another", "tristan"),
            () -> redis.lmove("another", "another", LMoveArgs.Builder.leftRight()));
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

   @Test
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

      RedisCodec<String, String> codec = StringCodec.UTF8;
      assertThatThrownBy(() ->
              // lmpop 1 mylist left count 1 count 2
              redis.dispatch(CommandType.LMPOP, new IntegerOutput<>(codec),
                      new CommandArgs<>(codec).add(1)
                              .addKey("mylist")
                              .add("left")
                              .add("count")
                              .add(1)
                              .add("count")
                              .add(2))).isInstanceOf(RedisCommandExecutionException.class)
              .hasMessageContaining("ERR syntax error");
   }

   @Test
   public void testLMPOPWithLowerCase() {
      RedisCodec<String, String> codec = StringCodec.UTF8;
      // lmpop 1 mylist left count 1
      redis.dispatch(CommandType.LMPOP, new IntegerOutput<>(codec),
              new CommandArgs<>(codec).add(1)
                      .addKey("mylist")
                      .add("left")
                      .add("count")
                      .add(1));
   }

   public void testLPOSMaxlen() {
      redis.rpush("mylist", "a", "b", "c", "d", "a", "b");
      // MAXLEN limits scan to first N elements
      assertThat(redis.lpos("mylist", "a", 0, LPosArgs.Builder.maxlen(4))).containsExactly(0L);
      assertThat(redis.lpos("mylist", "b", 0, LPosArgs.Builder.maxlen(5))).containsExactly(1L);
      assertThat(redis.lpos("mylist", "b", 0, LPosArgs.Builder.maxlen(6))).containsExactly(1L, 5L);
      // MAXLEN too small to find the element
      assertThat(redis.lpos("mylist", "a", 0, LPosArgs.Builder.maxlen(1))).containsExactly(0L);
      assertThat(redis.lpos("mylist", "d", 0, LPosArgs.Builder.maxlen(2))).isEmpty();
   }

   public void testLPOSCountPlusRank() {
      redis.rpush("mylist", "a", "b", "c", "d", "a", "b");
      // COUNT + RANK combined
      assertThat(redis.lpos("mylist", "a", 0, LPosArgs.Builder.rank(2))).containsExactly(4L);
      assertThat(redis.lpos("mylist", "a", 0, LPosArgs.Builder.rank(-1))).containsExactly(4L, 0L);
   }

   public void testLPOSRankGreaterThanMatches() {
      redis.rpush("mylist", "a", "b", "c", "a");
      // RANK 3 but only 2 'a' matches -> nil
      assertThat(redis.lpos("mylist", "a", LPosArgs.Builder.rank(3))).isNull();
   }

   public void testLRANGEInvertedIndexes() {
      redis.rpush("mylist", "a", "b", "c", "d", "e");
      // start > end yields empty
      assertThat(redis.lrange("mylist", 3, 1)).isEmpty();
   }

   public void testLRANGEOutOfRangeIndexes() {
      redis.rpush("mylist", "a", "b", "c");
      // Out of range indexes return full list
      assertThat(redis.lrange("mylist", -100, 100)).containsExactly("a", "b", "c");
      assertThat(redis.lrange("mylist", 0, -1)).containsExactly("a", "b", "c");
   }

   public void testLRANGEWrongType() {
      assertWrongType(() -> redis.set("strkey", "value"), () -> redis.lrange("strkey", 0, -1));
   }

   public void testLTRIMOutOfRangeNegativeEndIndex() {
      redis.rpush("mylist", "a", "b", "c", "d", "e");
      // LTRIM with out of range negative end: keep nothing
      assertThat(redis.ltrim("mylist", 0, -100)).isEqualTo("OK");
      assertThat(redis.exists("mylist")).isEqualTo(0);
   }

   public void testDelList() {
      redis.rpush("mylist", "a", "b", "c");
      assertThat(redis.exists("mylist")).isEqualTo(1);
      assertThat(redis.del("mylist")).isEqualTo(1);
      assertThat(redis.exists("mylist")).isEqualTo(0);
      assertThat(redis.llen("mylist")).isEqualTo(0);
   }

   public void testRPOPLPUSHNonListSrc() {
      redis.set("strkey", "value");
      assertThatThrownBy(() -> redis.rpoplpush("strkey", "dst"))
            .isInstanceOf(RedisCommandExecutionException.class)
            .hasMessageContaining("WRONGTYPE");
   }

   public void testRPOPLPUSHNonListDst() {
      redis.rpush("src", "a", "b");
      redis.set("strkey", "value");
      assertThatThrownBy(() -> redis.rpoplpush("src", "strkey"))
            .isInstanceOf(RedisCommandExecutionException.class)
            .hasMessageContaining("WRONGTYPE");
   }

   public void testRPOPLPUSHNonExistingSrc() {
      assertThat(redis.rpoplpush("nokey", "dst")).isNull();
      // dst should not be created
      assertThat(redis.exists("dst")).isEqualTo(0);
   }

   public void testLINSERTBadSyntax() {
      redis.rpush("mylist", "a", "b", "c");
      RedisCodec<String, String> codec = StringCodec.UTF8;
      // LINSERT with invalid position (not BEFORE/AFTER)
      assertThatThrownBy(() -> redis.dispatch(CommandType.LINSERT, new IntegerOutput<>(codec),
            new CommandArgs<>(codec).addKey("mylist").add("INVALID").add("b").add("x")))
            .isInstanceOf(RedisCommandExecutionException.class)
            .hasMessageContaining("ERR syntax error");
   }

   public void testLINSERTNonExistingKey() {
      // LINSERT on non-existing key returns 0
      assertThat(redis.linsert("nokey", true, "pivot", "value")).isEqualTo(0);
      assertThat(redis.exists("nokey")).isEqualTo(0);
   }

   public void testLREMIntEncodedObjects() {
      redis.rpush("mylist", "1", "2", "3", "2", "1", "2", "3");
      assertThat(redis.lrem("mylist", 2, "2")).isEqualTo(2);
      assertThat(redis.lrange("mylist", 0, -1)).containsExactly("1", "3", "1", "2", "3");
   }

   public void testMassRpopLpop() {
      // Push 1000 elements and verify mass pop
      String[] values = new String[1000];
      for (int i = 0; i < 1000; i++) {
         values[i] = String.valueOf(i);
      }
      redis.rpush("biglist", values);
      assertThat(redis.llen("biglist")).isEqualTo(1000);

      // Pop all from left
      for (int i = 0; i < 500; i++) {
         assertThat(redis.lpop("biglist")).isEqualTo(String.valueOf(i));
      }
      // Pop all from right
      for (int i = 999; i >= 500; i--) {
         assertThat(redis.rpop("biglist")).isEqualTo(String.valueOf(i));
      }
      assertThat(redis.llen("biglist")).isEqualTo(0);
   }

   public void testLMPOPIllegalArguments() {
      RedisCodec<String, String> codec = StringCodec.UTF8;

      // numkeys = 0
      assertThatThrownBy(() -> redis.dispatch(CommandType.LMPOP, new IntegerOutput<>(codec),
            new CommandArgs<>(codec).add(0).addKey("mylist").add("LEFT")))
            .isInstanceOf(RedisCommandExecutionException.class);

      // Invalid direction
      assertThatThrownBy(() -> redis.dispatch(CommandType.LMPOP, new IntegerOutput<>(codec),
            new CommandArgs<>(codec).add(1).addKey("mylist").add("UP")))
            .isInstanceOf(RedisCommandExecutionException.class)
            .hasMessageContaining("ERR syntax error");

      // Negative count
      assertThatThrownBy(() -> redis.dispatch(CommandType.LMPOP, new IntegerOutput<>(codec),
            new CommandArgs<>(codec).add(1).addKey("mylist").add("LEFT").add("COUNT").add(-1)))
            .isInstanceOf(RedisCommandExecutionException.class);
   }

   public void testLMPOPMultipleExistingLists() {
      redis.rpush("list1", "a", "b", "c");
      redis.rpush("list2", "d", "e", "f");

      // Should pop from first non-empty list
      KeyValue<String, List<String>> result = redis.lmpop(left().count(2), "list1", "list2");
      assertThat(result.getKey()).isEqualTo("list1");
      assertThat(result.getValue()).containsExactly("a", "b");

      result = redis.lmpop(right().count(1), "list1", "list2");
      assertThat(result.getKey()).isEqualTo("list1");
      assertThat(result.getValue()).containsExactly("c");

      // list1 is now empty, should pop from list2
      assertThat(redis.exists("list1")).isEqualTo(0);
      result = redis.lmpop(left().count(1), "list1", "list2");
      assertThat(result.getKey()).isEqualTo("list2");
      assertThat(result.getValue()).containsExactly("d");
   }

   public void testLPOPCountNonExistingKey() {
      // Lettuce returns empty list for count variant on non-existing key
      assertThat(redis.lpop("nokey", 5)).isEmpty();
      assertThat(redis.rpop("nokey", 5)).isEmpty();
   }

   public void testVariadicRPUSHLPUSH() {
      // Test that variadic push maintains order
      redis.rpush("mylist", "a", "b", "c");
      assertThat(redis.lrange("mylist", 0, -1)).containsExactly("a", "b", "c");

      redis.lpush("mylist", "d", "e", "f");
      // LPUSH pushes each element to head, so f is first
      assertThat(redis.lrange("mylist", 0, -1)).containsExactly("f", "e", "d", "a", "b", "c");
   }

   public void testLMOVENonExistingSrc() {
      // LMOVE with non-existing source should return nil and not create dst
      assertThat(redis.lmove("nokey", "dst", LMoveArgs.Builder.leftRight())).isNull();
      assertThat(redis.exists("dst")).isEqualTo(0);
   }

   public void testLPOPRPOPCountZero() {
      redis.rpush("mylist", "a", "b", "c");
      // count 0 returns empty list
      assertThat(redis.lpop("mylist", 0)).isEmpty();
      assertThat(redis.rpop("mylist", 0)).isEmpty();
      // List should be unchanged
      assertThat(redis.llen("mylist")).isEqualTo(3);
   }

   public void testLPOPRPOPCountMoreThanLen() {
      redis.rpush("mylist", "a", "b", "c");
      // Pop more than available returns all
      assertThat(redis.lpop("mylist", 10)).containsExactly("a", "b", "c");
      assertThat(redis.llen("mylist")).isEqualTo(0);
   }

   public void testLSETOutOfRange() {
      redis.rpush("mylist", "a", "b", "c");
      assertThatThrownBy(() -> redis.lset("mylist", 3, "d"))
            .isInstanceOf(RedisCommandExecutionException.class)
            .hasMessageContaining("ERR index out of range");
      assertThatThrownBy(() -> redis.lset("mylist", -4, "d"))
            .isInstanceOf(RedisCommandExecutionException.class)
            .hasMessageContaining("ERR index out of range");
   }

   public void testLSETNonExistingKey() {
      assertThatThrownBy(() -> redis.lset("nokey", 0, "value"))
            .isInstanceOf(RedisCommandExecutionException.class)
            .hasMessageContaining("ERR no such key");
   }

   public void testLINDEXConsistency() {
      // Push 1000 elements and verify random access
      for (int i = 0; i < 1000; i++) {
         redis.rpush("mylist", String.valueOf(i));
      }
      assertThat(redis.llen("mylist")).isEqualTo(1000);
      assertThat(redis.lindex("mylist", 0)).isEqualTo("0");
      assertThat(redis.lindex("mylist", 999)).isEqualTo("999");
      assertThat(redis.lindex("mylist", -1)).isEqualTo("999");
      assertThat(redis.lindex("mylist", 500)).isEqualTo("500");
   }

   public void testLLENNonExistingKey() {
      assertThat(redis.llen("nokey")).isEqualTo(0);
   }

   public void testLINDEXNonExistingKey() {
      assertThat(redis.lindex("nokey", 0)).isNull();
   }

   public void testRPOPLPUSHSameListRotate() {
      redis.rpush("mylist", "a", "b", "c");
      assertThat(redis.rpoplpush("mylist", "mylist")).isEqualTo("c");
      assertThat(redis.lrange("mylist", 0, -1)).containsExactly("c", "a", "b");
      assertThat(redis.rpoplpush("mylist", "mylist")).isEqualTo("b");
      assertThat(redis.lrange("mylist", 0, -1)).containsExactly("b", "c", "a");
   }

   public void testLMOVESameListAllDirections() {
      redis.rpush("mylist", "a", "b", "c");
      // LEFT LEFT = rotate left to left (no change)
      assertThat(redis.lmove("mylist", "mylist", LMoveArgs.Builder.leftLeft())).isEqualTo("a");
      assertThat(redis.lrange("mylist", 0, -1)).containsExactly("a", "b", "c");
      // RIGHT RIGHT = rotate right to right (no change)
      assertThat(redis.lmove("mylist", "mylist", LMoveArgs.Builder.rightRight())).isEqualTo("c");
      assertThat(redis.lrange("mylist", 0, -1)).containsExactly("a", "b", "c");
      // LEFT RIGHT = move head to tail
      assertThat(redis.lmove("mylist", "mylist", LMoveArgs.Builder.leftRight())).isEqualTo("a");
      assertThat(redis.lrange("mylist", 0, -1)).containsExactly("b", "c", "a");
      // RIGHT LEFT = move tail to head
      assertThat(redis.lmove("mylist", "mylist", LMoveArgs.Builder.rightLeft())).isEqualTo("a");
      assertThat(redis.lrange("mylist", 0, -1)).containsExactly("a", "b", "c");
   }

   public void testLMOVEWrongTypeDst() {
      redis.rpush("lmove-src", "a", "b");
      assertWrongType(() -> redis.set("lmove-strdst", "value"),
            () -> redis.lmove("lmove-src", "lmove-strdst", LMoveArgs.Builder.leftRight()));
   }

   public void testLREMCountLargerThanOccurrences() {
      redis.rpush("lrem-over", "a", "b", "a", "c", "a");
      // LREM with count=10 but only 3 occurrences of "a" - removes all, returns 3
      assertThat(redis.lrem("lrem-over", 10, "a")).isEqualTo(3);
      assertThat(redis.lrange("lrem-over", 0, -1)).containsExactly("b", "c");
   }

   public void testLREMNegativeCountLargerThanOccurrences() {
      redis.rpush("lrem-neg", "a", "b", "a", "c", "a");
      // LREM with count=-10 but only 3 occurrences - removes all from tail, returns 3
      assertThat(redis.lrem("lrem-neg", -10, "a")).isEqualTo(3);
      assertThat(redis.lrange("lrem-neg", 0, -1)).containsExactly("b", "c");
   }

   public void testLPOPEmptiesListRemovesKey() {
      redis.rpush("lpop-empty", "a");
      assertThat(redis.lpop("lpop-empty")).isEqualTo("a");
      assertThat(redis.exists("lpop-empty")).isEqualTo(0);
   }

   public void testRPOPEmptiesListRemovesKey() {
      redis.rpush("rpop-empty", "a");
      assertThat(redis.rpop("rpop-empty")).isEqualTo("a");
      assertThat(redis.exists("rpop-empty")).isEqualTo(0);
   }

   public void testLPOPCountEmptiesListRemovesKey() {
      redis.rpush("lpop-cnt-empty", "a", "b");
      assertThat(redis.lpop("lpop-cnt-empty", 5)).containsExactly("a", "b");
      assertThat(redis.exists("lpop-cnt-empty")).isEqualTo(0);
   }

   public void testLRANGENegativeEndIndex() {
      redis.rpush("lr-neg", "a", "b", "c", "d", "e");
      // -2 means exclude last element
      assertThat(redis.lrange("lr-neg", 0, -2)).containsExactly("a", "b", "c", "d");
      // -3 means exclude last 2
      assertThat(redis.lrange("lr-neg", 0, -3)).containsExactly("a", "b", "c");
      // Negative start and end
      assertThat(redis.lrange("lr-neg", -3, -1)).containsExactly("c", "d", "e");
   }

   public void testLTRIMKeepsSubset() {
      redis.rpush("ltrim-sub", "a", "b", "c", "d", "e");
      assertThat(redis.ltrim("ltrim-sub", 1, 3)).isEqualTo("OK");
      assertThat(redis.lrange("ltrim-sub", 0, -1)).containsExactly("b", "c", "d");
   }

   public void testLTRIMNegativeIndexes() {
      redis.rpush("ltrim-neg", "a", "b", "c", "d", "e");
      assertThat(redis.ltrim("ltrim-neg", -3, -1)).isEqualTo("OK");
      assertThat(redis.lrange("ltrim-neg", 0, -1)).containsExactly("c", "d", "e");
   }

   public void testLTRIMEmptiesListRemovesKey() {
      redis.rpush("ltrim-empty", "a", "b", "c");
      // Start > end empties the list
      assertThat(redis.ltrim("ltrim-empty", 3, 1)).isEqualTo("OK");
      assertThat(redis.exists("ltrim-empty")).isEqualTo(0);
   }

   public void testLINSERTBeforeFirstAfterLast() {
      redis.rpush("lins-edge", "b", "c", "d");
      // Insert before the first element
      assertThat(redis.linsert("lins-edge", true, "b", "a")).isEqualTo(4);
      assertThat(redis.lrange("lins-edge", 0, -1)).containsExactly("a", "b", "c", "d");
      // Insert after the last element
      assertThat(redis.linsert("lins-edge", false, "d", "e")).isEqualTo(5);
      assertThat(redis.lrange("lins-edge", 0, -1)).containsExactly("a", "b", "c", "d", "e");
   }

   public void testLMOVESourceBecomesEmpty() {
      redis.rpush("lmove-single", "only");
      assertThat(redis.lmove("lmove-single", "lmove-dst", LMoveArgs.Builder.leftRight())).isEqualTo("only");
      // Source should be deleted
      assertThat(redis.exists("lmove-single")).isEqualTo(0);
      // Destination should have the element
      assertThat(redis.lrange("lmove-dst", 0, -1)).containsExactly("only");
   }

   public void testRPOPLPUSHSourceBecomesEmpty() {
      redis.rpush("rpoplpush-single", "only");
      assertThat(redis.rpoplpush("rpoplpush-single", "rpoplpush-dst")).isEqualTo("only");
      assertThat(redis.exists("rpoplpush-single")).isEqualTo(0);
      assertThat(redis.lrange("rpoplpush-dst", 0, -1)).containsExactly("only");
   }

   public void testLMOVECreatesDst() {
      redis.rpush("lmove-csrc", "a", "b", "c");
      // LMOVE to non-existing dst should create it
      assertThat(redis.lmove("lmove-csrc", "lmove-cdst", LMoveArgs.Builder.rightLeft())).isEqualTo("c");
      assertThat(redis.lrange("lmove-cdst", 0, -1)).containsExactly("c");
   }

   public void testLPUSHXRPUSHXGeneric() {
      // LPUSHX/RPUSHX return 0 and don't create key when key doesn't exist
      assertThat(redis.lpushx("pushx-ne", "a")).isEqualTo(0);
      assertThat(redis.rpushx("pushx-ne", "a")).isEqualTo(0);
      assertThat(redis.exists("pushx-ne")).isEqualTo(0);

      // Wrong type
      assertWrongType(() -> redis.set("pushx-str", "value"), () -> redis.lpushx("pushx-str", "a"));
      assertWrongType(() -> redis.set("pushx-str2", "value"), () -> redis.rpushx("pushx-str2", "a"));
   }

   public void testLREMRemovesKeyWhenEmpty() {
      redis.rpush("lrem-delkey", "a", "a", "a");
      assertThat(redis.lrem("lrem-delkey", 0, "a")).isEqualTo(3);
      assertThat(redis.exists("lrem-delkey")).isEqualTo(0);
   }

   public void testLPOSCountZeroMaxlen() {
      redis.rpush("lpos-cm", "a", "b", "a", "c", "a", "d", "a");
      // COUNT 0 with MAXLEN: find all within first 5 elements
      assertThat(redis.lpos("lpos-cm", "a", 0, LPosArgs.Builder.maxlen(5))).containsExactly(0L, 2L, 4L);
      assertThat(redis.lpos("lpos-cm", "a", 0, LPosArgs.Builder.maxlen(3))).containsExactly(0L, 2L);
   }

   public void testLMPOPSingleExistingList() {
      redis.rpush("lmpop-single", "a", "b", "c", "d");

      // Pop from left
      KeyValue<String, List<String>> result = redis.lmpop(left().count(2), "lmpop-single");
      assertThat(result.getKey()).isEqualTo("lmpop-single");
      assertThat(result.getValue()).containsExactly("a", "b");

      // Pop from right
      result = redis.lmpop(right().count(2), "lmpop-single");
      assertThat(result.getKey()).isEqualTo("lmpop-single");
      assertThat(result.getValue()).containsExactly("d", "c");

      // List should be empty and removed
      assertThat(redis.exists("lmpop-single")).isEqualTo(0);
   }

   public void testLMPOPEmptiesKeyRemovesIt() {
      redis.rpush("lmpop-del", "a");
      KeyValue<String, List<String>> result = redis.lmpop(left(), "lmpop-del");
      assertThat(result.getValue()).containsExactly("a");
      assertThat(redis.exists("lmpop-del")).isEqualTo(0);
   }

   public void testRPOPLPUSHToExistingDst() {
      redis.rpush("rpoplpush-src2", "a", "b", "c");
      redis.rpush("rpoplpush-dst2", "x", "y");
      assertThat(redis.rpoplpush("rpoplpush-src2", "rpoplpush-dst2")).isEqualTo("c");
      assertThat(redis.lrange("rpoplpush-dst2", 0, -1)).containsExactly("c", "x", "y");
      assertThat(redis.lrange("rpoplpush-src2", 0, -1)).containsExactly("a", "b");
   }

   public void testLSETWrongType() {
      assertWrongType(() -> redis.set("lset-str", "value"), () -> redis.lset("lset-str", 0, "x"));
   }

   public void testLREMWrongType() {
      assertWrongType(() -> redis.set("lrem-str", "value"), () -> redis.lrem("lrem-str", 0, "x"));
   }

   public void testLTRIMWrongType() {
      assertWrongType(() -> redis.set("ltrim-str", "value"), () -> redis.ltrim("ltrim-str", 0, 1));
   }

   public void testLLENWrongType() {
      assertWrongType(() -> redis.set("llen-str", "value"), () -> redis.llen("llen-str"));
   }

   public void testLINDEXWrongType() {
      assertWrongType(() -> redis.set("lindex-str", "value"), () -> redis.lindex("lindex-str", 0));
   }
}
