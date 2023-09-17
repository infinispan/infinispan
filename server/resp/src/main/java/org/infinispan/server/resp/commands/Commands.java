package org.infinispan.server.resp.commands;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.commands.cluster.CLUSTER;
import org.infinispan.server.resp.commands.connection.AUTH;
import org.infinispan.server.resp.commands.connection.CLIENT;
import org.infinispan.server.resp.commands.connection.COMMAND;
import org.infinispan.server.resp.commands.connection.DBSIZE;
import org.infinispan.server.resp.commands.connection.ECHO;
import org.infinispan.server.resp.commands.connection.HELLO;
import org.infinispan.server.resp.commands.connection.MEMORY;
import org.infinispan.server.resp.commands.connection.MODULE;
import org.infinispan.server.resp.commands.connection.PING;
import org.infinispan.server.resp.commands.connection.QUIT;
import org.infinispan.server.resp.commands.connection.READONLY;
import org.infinispan.server.resp.commands.connection.READWRITE;
import org.infinispan.server.resp.commands.connection.RESET;
import org.infinispan.server.resp.commands.connection.SELECT;
import org.infinispan.server.resp.commands.generic.EXISTS;
import org.infinispan.server.resp.commands.generic.EXPIRE;
import org.infinispan.server.resp.commands.generic.EXPIREAT;
import org.infinispan.server.resp.commands.generic.EXPIRETIME;
import org.infinispan.server.resp.commands.generic.FLUSHALL;
import org.infinispan.server.resp.commands.generic.FLUSHDB;
import org.infinispan.server.resp.commands.generic.PERSIST;
import org.infinispan.server.resp.commands.generic.PEXPIRETIME;
import org.infinispan.server.resp.commands.generic.PTTL;
import org.infinispan.server.resp.commands.generic.SCAN;
import org.infinispan.server.resp.commands.generic.TTL;
import org.infinispan.server.resp.commands.generic.TYPE;
import org.infinispan.server.resp.commands.hash.HDEL;
import org.infinispan.server.resp.commands.hash.HEXISTS;
import org.infinispan.server.resp.commands.hash.HGET;
import org.infinispan.server.resp.commands.hash.HGETALL;
import org.infinispan.server.resp.commands.hash.HINCRBY;
import org.infinispan.server.resp.commands.hash.HINCRBYFLOAT;
import org.infinispan.server.resp.commands.hash.HKEYS;
import org.infinispan.server.resp.commands.hash.HLEN;
import org.infinispan.server.resp.commands.hash.HMGET;
import org.infinispan.server.resp.commands.hash.HMSET;
import org.infinispan.server.resp.commands.hash.HRANDFIELD;
import org.infinispan.server.resp.commands.hash.HSCAN;
import org.infinispan.server.resp.commands.hash.HSET;
import org.infinispan.server.resp.commands.hash.HVALS;
import org.infinispan.server.resp.commands.list.LINDEX;
import org.infinispan.server.resp.commands.list.LINSERT;
import org.infinispan.server.resp.commands.list.LLEN;
import org.infinispan.server.resp.commands.list.LMOVE;
import org.infinispan.server.resp.commands.list.LMPOP;
import org.infinispan.server.resp.commands.list.LPOP;
import org.infinispan.server.resp.commands.list.LPOS;
import org.infinispan.server.resp.commands.list.LPUSH;
import org.infinispan.server.resp.commands.list.LPUSHX;
import org.infinispan.server.resp.commands.list.LRANGE;
import org.infinispan.server.resp.commands.list.LREM;
import org.infinispan.server.resp.commands.list.LSET;
import org.infinispan.server.resp.commands.list.LTRIM;
import org.infinispan.server.resp.commands.list.RPOP;
import org.infinispan.server.resp.commands.list.RPOPLPUSH;
import org.infinispan.server.resp.commands.list.RPUSH;
import org.infinispan.server.resp.commands.list.RPUSHX;
import org.infinispan.server.resp.commands.pubsub.PSUBSCRIBE;
import org.infinispan.server.resp.commands.pubsub.PUBLISH;
import org.infinispan.server.resp.commands.pubsub.PUNSUBSCRIBE;
import org.infinispan.server.resp.commands.pubsub.SUBSCRIBE;
import org.infinispan.server.resp.commands.pubsub.UNSUBSCRIBE;
import org.infinispan.server.resp.commands.set.SADD;
import org.infinispan.server.resp.commands.set.SCARD;
import org.infinispan.server.resp.commands.set.SINTER;
import org.infinispan.server.resp.commands.set.SINTERCARD;
import org.infinispan.server.resp.commands.set.SINTERSTORE;
import org.infinispan.server.resp.commands.set.SMEMBERS;
import org.infinispan.server.resp.commands.set.SMOVE;
import org.infinispan.server.resp.commands.set.SPOP;
import org.infinispan.server.resp.commands.set.SRANDMEMBER;
import org.infinispan.server.resp.commands.set.SREM;
import org.infinispan.server.resp.commands.set.SUNION;
import org.infinispan.server.resp.commands.set.SUNIONSTORE;
import org.infinispan.server.resp.commands.sortedset.ZADD;
import org.infinispan.server.resp.commands.sortedset.ZCARD;
import org.infinispan.server.resp.commands.sortedset.ZCOUNT;
import org.infinispan.server.resp.commands.sortedset.ZDIFF;
import org.infinispan.server.resp.commands.sortedset.ZDIFFSTORE;
import org.infinispan.server.resp.commands.sortedset.ZINCRBY;
import org.infinispan.server.resp.commands.sortedset.ZINTER;
import org.infinispan.server.resp.commands.sortedset.ZINTERCARD;
import org.infinispan.server.resp.commands.sortedset.ZINTERSTORE;
import org.infinispan.server.resp.commands.sortedset.ZLEXCOUNT;
import org.infinispan.server.resp.commands.sortedset.ZMSCORE;
import org.infinispan.server.resp.commands.sortedset.ZPOPMAX;
import org.infinispan.server.resp.commands.sortedset.ZPOPMIN;
import org.infinispan.server.resp.commands.sortedset.ZRANDMEMBER;
import org.infinispan.server.resp.commands.sortedset.ZRANGE;
import org.infinispan.server.resp.commands.sortedset.ZRANGEBYLEX;
import org.infinispan.server.resp.commands.sortedset.ZRANGEBYSCORE;
import org.infinispan.server.resp.commands.sortedset.ZRANGESTORE;
import org.infinispan.server.resp.commands.sortedset.ZRANK;
import org.infinispan.server.resp.commands.sortedset.ZREM;
import org.infinispan.server.resp.commands.sortedset.ZREMRANGEBYLEX;
import org.infinispan.server.resp.commands.sortedset.ZREMRANGEBYRANK;
import org.infinispan.server.resp.commands.sortedset.ZREMRANGEBYSCORE;
import org.infinispan.server.resp.commands.sortedset.ZREVRANGE;
import org.infinispan.server.resp.commands.sortedset.ZREVRANGEBYLEX;
import org.infinispan.server.resp.commands.sortedset.ZREVRANGEBYSCORE;
import org.infinispan.server.resp.commands.sortedset.ZREVRANK;
import org.infinispan.server.resp.commands.sortedset.ZSCAN;
import org.infinispan.server.resp.commands.sortedset.ZSCORE;
import org.infinispan.server.resp.commands.sortedset.ZUNION;
import org.infinispan.server.resp.commands.sortedset.ZUNIONSTORE;
import org.infinispan.server.resp.commands.string.APPEND;
import org.infinispan.server.resp.commands.string.DECR;
import org.infinispan.server.resp.commands.string.DECRBY;
import org.infinispan.server.resp.commands.string.DEL;
import org.infinispan.server.resp.commands.string.GET;
import org.infinispan.server.resp.commands.string.GETDEL;
import org.infinispan.server.resp.commands.string.GETEX;
import org.infinispan.server.resp.commands.string.GETRANGE;
import org.infinispan.server.resp.commands.string.INCR;
import org.infinispan.server.resp.commands.string.INCRBY;
import org.infinispan.server.resp.commands.string.INCRBYFLOAT;
import org.infinispan.server.resp.commands.string.MGET;
import org.infinispan.server.resp.commands.string.MSET;
import org.infinispan.server.resp.commands.string.MSETNX;
import org.infinispan.server.resp.commands.string.SET;
import org.infinispan.server.resp.commands.string.SETRANGE;
import org.infinispan.server.resp.commands.string.STRALGO;
import org.infinispan.server.resp.commands.string.STRLEN;
import org.infinispan.server.resp.commands.tx.DISCARD;
import org.infinispan.server.resp.commands.tx.EXEC;
import org.infinispan.server.resp.commands.tx.MULTI;
import org.infinispan.server.resp.commands.tx.UNWATCH;
import org.infinispan.server.resp.commands.tx.WATCH;

/**
 * @since 15.0
 **/
public final class Commands {
   public static final RespCommand[][] ALL_COMMANDS;

   static {
      ALL_COMMANDS = new RespCommand[26][];
      // Just manual for now, but we may want to dynamically at some point.
      // NOTE that the order within the sub array matters, commands we want to have the lowest latency should be first
      // in this array as they are looked up sequentially for matches
      ALL_COMMANDS[0] = new RespCommand[]{new APPEND(), new AUTH()};
      ALL_COMMANDS[2] = new RespCommand[]{new CONFIG(), new COMMAND(), new CLUSTER(), new CLIENT() };
      // DEL should always be first here
      ALL_COMMANDS[3] = new RespCommand[]{new DEL(), new DECR(), new DECRBY(), new DBSIZE(), new DISCARD()};
      ALL_COMMANDS[4] = new RespCommand[]{new ECHO(), new EXISTS(), new EXPIRE(), new EXPIREAT(), new EXPIRETIME(), new EXEC()};
      ALL_COMMANDS[5] = new RespCommand[]{new FLUSHDB(), new FLUSHALL()};
      // GET should always be first here
      ALL_COMMANDS[6] = new RespCommand[]{new GET(), new GETDEL(), new GETEX(), new GETRANGE()};
      ALL_COMMANDS[7] = new RespCommand[]{new HELLO(), new HGET(), new HSET(), new HLEN(), new HEXISTS(), new HDEL(), new HMGET(), new HKEYS(), new HVALS(), new HSCAN(), new HGETALL(), new HMSET(), new HINCRBY(), new HINCRBYFLOAT(), new HRANDFIELD()};
      ALL_COMMANDS[8] = new RespCommand[]{new INCR(), new INCRBY(), new INCRBYFLOAT(), new INFO()};
      ALL_COMMANDS[11] = new RespCommand[]{new LINDEX(), new LINSERT(), new LPUSH(), new LPUSHX(), new LPOP(), new LRANGE(), new LLEN(), new LPOS(), new LREM(), new LSET(), new LTRIM(), new LMOVE(), new LMPOP() };
      ALL_COMMANDS[12] = new RespCommand[]{new MGET(), new MSET(), new MSETNX(), new MULTI(), new MODULE(), new MEMORY()};
      ALL_COMMANDS[15] = new RespCommand[]{new PUBLISH(), new PING(), new PSUBSCRIBE(), new PUNSUBSCRIBE(), new PTTL(), new PEXPIRETIME(), new PERSIST()};
      ALL_COMMANDS[16] = new RespCommand[]{new QUIT()};
      ALL_COMMANDS[17] = new RespCommand[]{new RPUSH(), new RPUSHX(), new RPOP(), new RESET(), new READWRITE(), new READONLY(), new RPOPLPUSH() };
      // SET should always be first here
      ALL_COMMANDS[18] = new RespCommand[]{new SET(), new SMEMBERS(), new SADD(), new STRLEN(), new SMOVE(), new SCARD(), new SINTER(), new SINTERSTORE(), new SINTERCARD(), new SUNION(), new SUNIONSTORE(), new SPOP(), new SRANDMEMBER(), new SREM(), new SUBSCRIBE(), new SELECT(), new STRALGO(), new SCAN(), new SETRANGE()};
      ALL_COMMANDS[19] = new RespCommand[]{new TTL(), new TYPE()};
      ALL_COMMANDS[20] = new RespCommand[]{new UNSUBSCRIBE(), new UNWATCH()};
      ALL_COMMANDS[22] = new RespCommand[]{new WATCH()};
      ALL_COMMANDS[25] = new RespCommand[]{new ZADD(), new ZCARD(), new ZCOUNT(), new ZLEXCOUNT(), new ZDIFF(), new ZDIFFSTORE(), new ZINCRBY(), new ZINTER(), new ZINTERCARD(), new ZINTERSTORE(),
            new ZPOPMAX(), new ZPOPMIN(), new ZRANGE(), new ZRANGESTORE(), new ZREVRANGE(), new ZRANGEBYSCORE(), new ZRANK(), new ZREVRANGEBYSCORE(),
            new ZRANGEBYLEX(), new ZREVRANGEBYLEX(), new ZREVRANK(),  new ZREM(), new ZREMRANGEBYRANK(), new ZREMRANGEBYLEX(), new ZREMRANGEBYSCORE(),
            new ZSCORE(), new ZMSCORE(), new ZUNION(), new ZUNIONSTORE(), new ZRANDMEMBER(), new ZSCAN() };
   }

   public static List<RespCommand> all() {
      List<RespCommand> respCommands = new ArrayList<>();

      for (RespCommand[] commands : ALL_COMMANDS) {
         if (commands != null) {
            respCommands.addAll(Arrays.asList(commands));
         }
      }
      return respCommands;
   }
}
