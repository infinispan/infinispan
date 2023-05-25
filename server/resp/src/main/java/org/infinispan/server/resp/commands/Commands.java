package org.infinispan.server.resp.commands;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.commands.connection.AUTH;
import org.infinispan.server.resp.commands.connection.COMMAND;
import org.infinispan.server.resp.commands.connection.DBSIZE;
import org.infinispan.server.resp.commands.connection.ECHO;
import org.infinispan.server.resp.commands.connection.HELLO;
import org.infinispan.server.resp.commands.connection.MODULE;
import org.infinispan.server.resp.commands.connection.PING;
import org.infinispan.server.resp.commands.connection.QUIT;
import org.infinispan.server.resp.commands.connection.READONLY;
import org.infinispan.server.resp.commands.connection.READWRITE;
import org.infinispan.server.resp.commands.connection.RESET;
import org.infinispan.server.resp.commands.connection.SELECT;
import org.infinispan.server.resp.commands.generic.EXISTS;
import org.infinispan.server.resp.commands.generic.FLUSHALL;
import org.infinispan.server.resp.commands.generic.FLUSHDB;
import org.infinispan.server.resp.commands.generic.SCAN;
import org.infinispan.server.resp.commands.list.LINDEX;
import org.infinispan.server.resp.commands.list.LINSERT;
import org.infinispan.server.resp.commands.list.LLEN;
import org.infinispan.server.resp.commands.list.LPOP;
import org.infinispan.server.resp.commands.list.LPOS;
import org.infinispan.server.resp.commands.list.LPUSH;
import org.infinispan.server.resp.commands.list.LPUSHX;
import org.infinispan.server.resp.commands.list.LRANGE;
import org.infinispan.server.resp.commands.list.LSET;
import org.infinispan.server.resp.commands.list.RPOP;
import org.infinispan.server.resp.commands.list.RPUSH;
import org.infinispan.server.resp.commands.list.RPUSHX;
import org.infinispan.server.resp.commands.pubsub.PSUBSCRIBE;
import org.infinispan.server.resp.commands.pubsub.PUBLISH;
import org.infinispan.server.resp.commands.pubsub.PUNSUBSCRIBE;
import org.infinispan.server.resp.commands.pubsub.SUBSCRIBE;
import org.infinispan.server.resp.commands.pubsub.UNSUBSCRIBE;
import org.infinispan.server.resp.commands.string.APPEND;
import org.infinispan.server.resp.commands.string.DECR;
import org.infinispan.server.resp.commands.string.DECRBY;
import org.infinispan.server.resp.commands.string.DEL;
import org.infinispan.server.resp.commands.string.GET;
import org.infinispan.server.resp.commands.string.GETDEL;
import org.infinispan.server.resp.commands.string.INCR;
import org.infinispan.server.resp.commands.string.INCRBY;
import org.infinispan.server.resp.commands.string.INCRBYFLOAT;
import org.infinispan.server.resp.commands.string.MGET;
import org.infinispan.server.resp.commands.string.MSET;
import org.infinispan.server.resp.commands.string.SET;
import org.infinispan.server.resp.commands.string.STRALGO;
import org.infinispan.server.resp.commands.string.STRLEN;

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
      ALL_COMMANDS[2] = new RespCommand[]{new CONFIG(), new COMMAND()};
      // DEL should always be first here
      ALL_COMMANDS[3] = new RespCommand[]{new DEL(), new DECR(), new DECRBY(), new DBSIZE()};
      ALL_COMMANDS[4] = new RespCommand[]{new ECHO(), new EXISTS()};
      ALL_COMMANDS[5] = new RespCommand[]{new FLUSHDB(), new FLUSHALL()};
      // GET should always be first here
      ALL_COMMANDS[6] = new RespCommand[]{new GET(), new GETDEL()};
      ALL_COMMANDS[7] = new RespCommand[]{new HELLO()};
      ALL_COMMANDS[8] = new RespCommand[]{new INCR(), new INCRBY(), new INCRBYFLOAT(), new INFO()};
      ALL_COMMANDS[11] = new RespCommand[]{new LINDEX(), new LINSERT(), new LPUSH(), new LPUSHX(), new LPOP(), new LRANGE(), new LLEN(), new LPOS(), new LSET() };
      ALL_COMMANDS[12] = new RespCommand[]{new MGET(), new MSET(), new MODULE()};
      ALL_COMMANDS[15] = new RespCommand[]{new PUBLISH(), new PING(), new PSUBSCRIBE(), new PUNSUBSCRIBE()};
      ALL_COMMANDS[16] = new RespCommand[]{new QUIT()};
      ALL_COMMANDS[17] = new RespCommand[]{new RPUSH(), new RPUSHX(), new RPOP(), new RESET(), new READWRITE(), new READONLY()};
      // SET should always be first here
      ALL_COMMANDS[18] = new RespCommand[]{new SET(), new STRLEN(), new SUBSCRIBE(), new SELECT(), new STRALGO(), new SCAN()};
      ALL_COMMANDS[20] = new RespCommand[]{new UNSUBSCRIBE()};
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
