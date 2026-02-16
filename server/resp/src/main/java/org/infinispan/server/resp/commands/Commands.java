package org.infinispan.server.resp.commands;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.commands.bitmap.BITCOUNT;
import org.infinispan.server.resp.commands.bitmap.BITFIELD;
import org.infinispan.server.resp.commands.bitmap.BITFIELD_RO;
import org.infinispan.server.resp.commands.bitmap.BITOP;
import org.infinispan.server.resp.commands.bitmap.BITPOS;
import org.infinispan.server.resp.commands.bitmap.GETBIT;
import org.infinispan.server.resp.commands.bitmap.SETBIT;
import org.infinispan.server.resp.commands.bloom.BFADD;
import org.infinispan.server.resp.commands.bloom.BFCARD;
import org.infinispan.server.resp.commands.bloom.BFEXISTS;
import org.infinispan.server.resp.commands.bloom.BFINFO;
import org.infinispan.server.resp.commands.bloom.BFINSERT;
import org.infinispan.server.resp.commands.bloom.BFMADD;
import org.infinispan.server.resp.commands.bloom.BFMEXISTS;
import org.infinispan.server.resp.commands.bloom.BFRESERVE;
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
import org.infinispan.server.resp.commands.generic.KEYS;
import org.infinispan.server.resp.commands.generic.LOLWUT;
import org.infinispan.server.resp.commands.generic.PERSIST;
import org.infinispan.server.resp.commands.generic.PEXPIRE;
import org.infinispan.server.resp.commands.generic.PEXPIREAT;
import org.infinispan.server.resp.commands.generic.PEXPIRETIME;
import org.infinispan.server.resp.commands.generic.PTTL;
import org.infinispan.server.resp.commands.generic.RANDOMKEY;
import org.infinispan.server.resp.commands.generic.RENAME;
import org.infinispan.server.resp.commands.generic.RENAMENX;
import org.infinispan.server.resp.commands.generic.SCAN;
import org.infinispan.server.resp.commands.generic.SORT;
import org.infinispan.server.resp.commands.generic.SORT_RO;
import org.infinispan.server.resp.commands.generic.TIME;
import org.infinispan.server.resp.commands.generic.TOUCH;
import org.infinispan.server.resp.commands.generic.TTL;
import org.infinispan.server.resp.commands.generic.TYPE;
import org.infinispan.server.resp.commands.geo.GEOADD;
import org.infinispan.server.resp.commands.geo.GEODIST;
import org.infinispan.server.resp.commands.geo.GEOHASH;
import org.infinispan.server.resp.commands.geo.GEOPOS;
import org.infinispan.server.resp.commands.geo.GEORADIUS;
import org.infinispan.server.resp.commands.geo.GEORADIUSBYMEMBER;
import org.infinispan.server.resp.commands.geo.GEOSEARCH;
import org.infinispan.server.resp.commands.geo.GEOSEARCHSTORE;
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
import org.infinispan.server.resp.commands.hash.HSETNX;
import org.infinispan.server.resp.commands.hash.HSTRLEN;
import org.infinispan.server.resp.commands.hash.HVALS;
import org.infinispan.server.resp.commands.hll.PFADD;
import org.infinispan.server.resp.commands.hll.PFCOUNT;
import org.infinispan.server.resp.commands.hll.PFMERGE;
import org.infinispan.server.resp.commands.json.JSONARRAPPEND;
import org.infinispan.server.resp.commands.json.JSONARRINDEX;
import org.infinispan.server.resp.commands.json.JSONARRINSERT;
import org.infinispan.server.resp.commands.json.JSONARRLEN;
import org.infinispan.server.resp.commands.json.JSONARRPOP;
import org.infinispan.server.resp.commands.json.JSONARRTRIM;
import org.infinispan.server.resp.commands.json.JSONCLEAR;
import org.infinispan.server.resp.commands.json.JSONDEBUG;
import org.infinispan.server.resp.commands.json.JSONDEL;
import org.infinispan.server.resp.commands.json.JSONFORGET;
import org.infinispan.server.resp.commands.json.JSONGET;
import org.infinispan.server.resp.commands.json.JSONMERGE;
import org.infinispan.server.resp.commands.json.JSONMGET;
import org.infinispan.server.resp.commands.json.JSONMSET;
import org.infinispan.server.resp.commands.json.JSONNUMINCRBY;
import org.infinispan.server.resp.commands.json.JSONNUMMULTBY;
import org.infinispan.server.resp.commands.json.JSONOBJKEYS;
import org.infinispan.server.resp.commands.json.JSONOBJLEN;
import org.infinispan.server.resp.commands.json.JSONRESP;
import org.infinispan.server.resp.commands.json.JSONSET;
import org.infinispan.server.resp.commands.json.JSONSTRAPPEND;
import org.infinispan.server.resp.commands.json.JSONSTRLEN;
import org.infinispan.server.resp.commands.json.JSONTOGGLE;
import org.infinispan.server.resp.commands.json.JSONTYPE;
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
import org.infinispan.server.resp.commands.list.blocking.BLMPOP;
import org.infinispan.server.resp.commands.list.blocking.BLPOP;
import org.infinispan.server.resp.commands.list.blocking.BRPOP;
import org.infinispan.server.resp.commands.pubsub.PSUBSCRIBE;
import org.infinispan.server.resp.commands.pubsub.PUBLISH;
import org.infinispan.server.resp.commands.pubsub.PUBSUB;
import org.infinispan.server.resp.commands.pubsub.PUNSUBSCRIBE;
import org.infinispan.server.resp.commands.pubsub.SUBSCRIBE;
import org.infinispan.server.resp.commands.pubsub.UNSUBSCRIBE;
import org.infinispan.server.resp.commands.scripting.eval.EVAL;
import org.infinispan.server.resp.commands.scripting.eval.EVALSHA;
import org.infinispan.server.resp.commands.scripting.eval.EVALSHA_RO;
import org.infinispan.server.resp.commands.scripting.eval.EVAL_RO;
import org.infinispan.server.resp.commands.scripting.eval.SCRIPT;
import org.infinispan.server.resp.commands.search.FT_LIST;
import org.infinispan.server.resp.commands.set.SADD;
import org.infinispan.server.resp.commands.set.SCARD;
import org.infinispan.server.resp.commands.set.SDIFF;
import org.infinispan.server.resp.commands.set.SDIFFSTORE;
import org.infinispan.server.resp.commands.set.SINTER;
import org.infinispan.server.resp.commands.set.SINTERCARD;
import org.infinispan.server.resp.commands.set.SINTERSTORE;
import org.infinispan.server.resp.commands.set.SISMEMBER;
import org.infinispan.server.resp.commands.set.SMEMBERS;
import org.infinispan.server.resp.commands.set.SMISMEMBER;
import org.infinispan.server.resp.commands.set.SMOVE;
import org.infinispan.server.resp.commands.set.SPOP;
import org.infinispan.server.resp.commands.set.SRANDMEMBER;
import org.infinispan.server.resp.commands.set.SREM;
import org.infinispan.server.resp.commands.set.SSCAN;
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
import org.infinispan.server.resp.commands.sortedset.ZMPOP;
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
import org.infinispan.server.resp.commands.string.DELEX;
import org.infinispan.server.resp.commands.string.DIGEST;
import org.infinispan.server.resp.commands.string.GET;
import org.infinispan.server.resp.commands.string.GETDEL;
import org.infinispan.server.resp.commands.string.GETEX;
import org.infinispan.server.resp.commands.string.GETRANGE;
import org.infinispan.server.resp.commands.string.GETSET;
import org.infinispan.server.resp.commands.string.INCR;
import org.infinispan.server.resp.commands.string.INCRBY;
import org.infinispan.server.resp.commands.string.INCRBYFLOAT;
import org.infinispan.server.resp.commands.string.LCS;
import org.infinispan.server.resp.commands.string.MGET;
import org.infinispan.server.resp.commands.string.MSET;
import org.infinispan.server.resp.commands.string.MSETNX;
import org.infinispan.server.resp.commands.string.PSETEX;
import org.infinispan.server.resp.commands.string.SET;
import org.infinispan.server.resp.commands.string.SETEX;
import org.infinispan.server.resp.commands.string.SETNX;
import org.infinispan.server.resp.commands.string.SETRANGE;
import org.infinispan.server.resp.commands.string.STRALGO;
import org.infinispan.server.resp.commands.string.STRLEN;
import org.infinispan.server.resp.commands.string.SUBSTR;
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
      ALL_COMMANDS[1] = new RespCommand[]{new BLPOP(), new BRPOP(), new BLMPOP(), new BFADD(), new BFMADD(), new BFEXISTS(), new BFMEXISTS(), new BFRESERVE(), new BFINSERT(), new BFINFO(), new BFCARD(), new BITFIELD(), new BITFIELD_RO(), new BITCOUNT(), new BITOP(), new BITPOS()};
      ALL_COMMANDS[2] = new RespCommand[]{new CONFIG(), new COMMAND(), new CLUSTER(), new CLIENT()};
      // DEL should always be first here
      ALL_COMMANDS[3] = new RespCommand[]{new DEL(), new DECR(), new DECRBY(), new DBSIZE(), new DELEX(), new DIGEST(), new DISCARD()};
      ALL_COMMANDS[4] = new RespCommand[]{new ECHO(), new EXISTS(), new EXPIRE(), new EXPIREAT(), new EXPIRETIME(), new EXEC(), new EVAL(), new EVAL_RO(), new EVALSHA(), new EVALSHA_RO()};
      ALL_COMMANDS[5] = new RespCommand[]{new FLUSHDB(), new FLUSHALL(), new FT_LIST()};
      // GET should always be first here
      ALL_COMMANDS[6] = new RespCommand[]{new GET(), new GETDEL(), new GETEX(), new GETRANGE(), new GETSET(), new GETBIT(),
            new GEOADD(), new GEODIST(), new GEOHASH(), new GEOPOS(), new GEORADIUS(), new GEORADIUSBYMEMBER(),
            new GEOSEARCH(), new GEOSEARCHSTORE()};
      ALL_COMMANDS[7] = new RespCommand[]{new HELLO(), new HGET(), new HSET(), new HLEN(), new HEXISTS(), new HDEL(), new HMGET(), new HSETNX(), new HKEYS(), new HVALS(), new HSCAN(), new HGETALL(), new HMSET(), new HINCRBY(), new HINCRBYFLOAT(), new HRANDFIELD(), new HSTRLEN()};
      ALL_COMMANDS[8] = new RespCommand[]{new INCR(), new INCRBY(), new INCRBYFLOAT(), new INFO()};
      ALL_COMMANDS[9] = new RespCommand[]{new JSONGET(), new JSONSET(), new JSONARRLEN(), new JSONOBJLEN(), new JSONSTRLEN(), new JSONTYPE(), new JSONDEL(), new JSONSTRAPPEND(), new JSONARRAPPEND(), new JSONTOGGLE(), new JSONOBJKEYS(), new JSONNUMINCRBY(), new JSONNUMMULTBY(), new JSONFORGET(), new JSONARRINDEX(), new JSONARRINSERT(), new JSONARRTRIM(), new JSONCLEAR(), new JSONARRPOP(), new JSONMSET(), new JSONMERGE(), new JSONMGET(), new JSONRESP(), new JSONDEBUG()};
      ALL_COMMANDS[10] = new RespCommand[]{new KEYS()};
      ALL_COMMANDS[11] = new RespCommand[]{new LINDEX(), new LINSERT(), new LPUSH(), new LPUSHX(), new LPOP(), new LRANGE(), new LLEN(), new LPOS(), new LREM(), new LSET(), new LTRIM(), new LMOVE(), new LMPOP(), new LCS(), new LOLWUT()};
      ALL_COMMANDS[12] = new RespCommand[]{new MGET(), new MSET(), new MSETNX(), new MULTI(), new MODULE(), new MEMORY()};
      ALL_COMMANDS[15] = new RespCommand[]{new PUBLISH(), new PING(), new PSUBSCRIBE(), new PUNSUBSCRIBE(), new PUBSUB(), new PTTL(), new PEXPIREAT(), new PEXPIRE(), new PEXPIRETIME(), new PERSIST(), new PFADD(), new PFCOUNT(), new PFMERGE(), new PSETEX()};
      ALL_COMMANDS[16] = new RespCommand[]{new QUIT()};
      ALL_COMMANDS[17] = new RespCommand[]{new RPUSH(), new RPUSHX(), new RPOP(), new RESET(), new READWRITE(), new READONLY(), new RPOPLPUSH(), new RENAME(), new RENAMENX(), new RANDOMKEY()};
      // SET should always be first here
      ALL_COMMANDS[18] = new RespCommand[]{new SET(), new SETEX(), new SETNX(), new SMEMBERS(), new SISMEMBER(),
            new SMISMEMBER(), new SADD(), new STRLEN(), new SMOVE(), new SCARD(), new SINTER(), new SINTERSTORE(),
            new SINTERCARD(), new SUNION(), new SUNIONSTORE(), new SPOP(), new SRANDMEMBER(), new SREM(), new SDIFF(),
            new SDIFFSTORE(), new SUBSCRIBE(), new SELECT(), new STRALGO(), new SCAN(), new SSCAN(), new SETRANGE(),
            new SORT(), new SORT_RO(), new SUBSTR(), new SCRIPT(), new SETBIT()};
      ALL_COMMANDS[19] = new RespCommand[]{new TTL(), new TYPE(), new TOUCH(), new TIME()};
      ALL_COMMANDS[20] = new RespCommand[]{new UNSUBSCRIBE(), new UNWATCH()};
      ALL_COMMANDS[22] = new RespCommand[]{new WATCH()};
      ALL_COMMANDS[25] = new RespCommand[]{new ZADD(), new ZCARD(), new ZCOUNT(), new ZLEXCOUNT(), new ZDIFF(),
            new ZDIFFSTORE(), new ZINCRBY(), new ZINTER(), new ZINTERCARD(), new ZINTERSTORE(), new ZMPOP(),
            new ZPOPMAX(), new ZPOPMIN(), new ZRANGE(), new ZRANGESTORE(), new ZREVRANGE(), new ZRANGEBYSCORE(),
            new ZRANK(), new ZREVRANGEBYSCORE(), new ZRANGEBYLEX(), new ZREVRANGEBYLEX(), new ZREVRANK(), new ZREM(),
            new ZREMRANGEBYRANK(), new ZREMRANGEBYLEX(), new ZREMRANGEBYSCORE(), new ZSCORE(), new ZMSCORE(),
            new ZUNION(), new ZUNIONSTORE(), new ZRANDMEMBER(), new ZSCAN()};
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
