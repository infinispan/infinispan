package org.infinispan.server.resp.commands.connection;

import static org.infinispan.server.resp.RespConstants.CRLF_STRING;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CompletionStage;

import org.infinispan.security.AuthorizationPermission;
import org.infinispan.server.resp.ByteBufferUtils;
import org.infinispan.server.resp.Consumers;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespErrorUtil;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.Resp3Command;

import io.netty.channel.ChannelHandlerContext;

/**
 * <a href="https://redis.io/commands/memory/">MEMORY</a>
 *
 * @since 15.0
 */
public class MEMORY extends RespCommand implements Resp3Command {
   public MEMORY() {
      super(-2, 0, 0, 0);
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler, ChannelHandlerContext ctx,
                                                      List<byte[]> arguments) {
      handler.checkPermission(AuthorizationPermission.ADMIN);
      String subcommand = new String(arguments.get(0), StandardCharsets.US_ASCII).toUpperCase();
      switch (subcommand) {
         case "STATS":
            StringBuilder sb = new StringBuilder();
            sb.append("*54\r\n");
            addStat(sb, "peak.allocated", 0); //1
            addStat(sb, "total.allocated", 0); //3
            addStat(sb, "startup.allocated", 0); //5
            addStat(sb, "replication.backlog", 0);//7
            addStat(sb, "clients.slaves", 0); //9
            addStat(sb, "clients.normal", 0); //11
            addStat(sb, "cluster.links", 0); //13
            addStat(sb, "aof.buffer", 0); //15
            addStat(sb, "lua.caches", 0); //17
            addStat(sb, "functions.caches", 0); //19
            addStat(sb, "overhead.total", 0); //21
            addStat(sb, "keys.count", 0); //23
            addStat(sb, "keys.bytes-per-key", 0); //25
            addStat(sb, "dataset.bytes", 0); //27
            addStat(sb, "dataset.percentage", "0.0"); //29
            addStat(sb, "peak.percentage", "0.0"); //31
            addStat(sb, "allocator.allocated", 0); //33
            addStat(sb, "allocator.active", 0); //35
            addStat(sb, "allocator.resident", 0); //37
            addStat(sb, "allocator-fragmentation.ratio", "0.0"); //39
            addStat(sb, "allocator-fragmentation.bytes", 0); //41
            addStat(sb, "allocator-rss.ratio", "0.0"); //43
            addStat(sb, "allocator-rss.bytes", 0); //45
            addStat(sb, "rss-overhead.ratio", "0.0"); //47
            addStat(sb, "rss-overhead.bytes", 0); //49
            addStat(sb, "fragmentation", "0.0"); //51
            addStat(sb, "fragmentation.bytes", 0); //53
            ByteBufferUtils.stringToByteBufAscii(sb, handler.allocator());
            break;
         case "USAGE":
            if (arguments.size() < 2) {
               RespErrorUtil.wrongArgumentCount(this, handler.allocator());
               return handler.myStage();
            } else {
               byte[] key = arguments.get(1);
               return handler.stageToReturn(handler.cache().getAsync(key).thenApply(v ->
                           v == null ? null : (long) (key.length + v.length + 14) / 8 * 8),
                     ctx, Consumers.LONG_BICONSUMER);
            }
         case "DOCTOR":
         case "MALLOC-STATS":
         case "PURGE":
            ByteBufferUtils.stringToByteBufAscii("-ERR module loading/unloading unsupported\r\n", handler.allocator());
            break;
      }
      return handler.myStage();
   }

   private void addStat(StringBuilder sb, String s, int i) {
      sb.append('$');
      sb.append(s.length());
      sb.append(CRLF_STRING);
      sb.append(s);
      sb.append(CRLF_STRING);
      sb.append(':');
      sb.append(i);
      sb.append(CRLF_STRING);
   }

   private void addStat(StringBuilder sb, String s, String v) {
      sb.append('$');
      sb.append(s.length());
      sb.append(CRLF_STRING);
      sb.append(s);
      sb.append(CRLF_STRING);
      sb.append('$');
      sb.append(v.length());
      sb.append(CRLF_STRING);
      sb.append(v);
      sb.append(CRLF_STRING);
   }
}
