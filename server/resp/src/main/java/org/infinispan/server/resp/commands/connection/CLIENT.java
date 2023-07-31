package org.infinispan.server.resp.commands.connection;

import static org.infinispan.server.resp.Resp3Handler.handleBulkResult;
import static org.infinispan.server.resp.Util.utf8;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CompletionStage;

import org.infinispan.security.AuthorizationPermission;
import org.infinispan.security.Security;
import org.infinispan.server.core.transport.ConnectionMetadata;
import org.infinispan.server.core.transport.NettyTransport;
import org.infinispan.server.resp.ByteBufferUtils;
import org.infinispan.server.resp.Consumers;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.Resp3Command;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.ChannelMatcher;

/**
 * <a href="https://redis.io/commands/client/">CLIENT</a> *
 *
 * @since 15.0
 */
public class CLIENT extends RespCommand implements Resp3Command {
   public CLIENT() {
      super(-2, 0, 0, 0);
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler, ChannelHandlerContext ctx,
                                                      List<byte[]> arguments) {
      String subcommand = utf8(arguments.get(0)).toUpperCase();
      ConnectionMetadata metadata = ConnectionMetadata.getInstance(ctx.channel());
      switch (subcommand) {
         case "CACHING":
         case "UNPAUSE":
         case "PAUSE":
         case "NO-EVICT":
         case "KILL":
         case "NO-TOUCH":
         case "GETREDIR":
         case "UNBLOCK":
         case "REPLY":
            ByteBufferUtils.stringToByteBufAscii("-ERR unsupported command\r\n", handler.allocator());
            break;
         case "SETINFO":
            for (int i = 1; i < arguments.size(); i++) {
               String name = utf8(arguments.get(i));
               switch (name.toUpperCase()) {
                  case "LIB-NAME":
                     metadata.clientLibraryName(utf8(arguments.get(++i)));
                     break;
                  case "LIB-VER":
                     metadata.clientLibraryVersion(utf8(arguments.get(++i)));
                     break;
                  default:
                     ByteBufferUtils.stringToByteBuf("-ERR unsupported attribute " + name + "\r\n", handler.allocator());
                     return handler.myStage();
               }
            }
            break;
         case "SETNAME":
            metadata.clientName(utf8(arguments.get(1)));
            Consumers.OK_BICONSUMER.accept(null, handler.allocator());
            break;
         case "GETNAME":
            // This could be UTF-8
            handleBulkResult(metadata.clientName(), handler.allocator());
            break;
         case "ID":
            Consumers.LONG_BICONSUMER.accept(metadata.id(), handler.allocator());
            break;
         case "INFO": {
            StringBuilder sb = new StringBuilder();
            addInfo(sb, metadata);
            // This could be UTF-8
            handleBulkResult(sb, handler.allocator());
            break;
         }
         case "LIST": {
            handler.checkPermission(AuthorizationPermission.ADMIN);
            StringBuilder sb = new StringBuilder();
            ChannelMatcher matcher = handler.respServer().getChannelMatcher();
            NettyTransport transport = handler.respServer().getTransport();
            if (transport == null) {
               transport = (NettyTransport) handler.respServer().getEnclosingProtocolServer().getTransport();
            }
            ChannelGroup channels = transport.getAcceptedChannels();
            channels.forEach(ch -> {
               if (matcher.matches(ch)) {
                  addInfo(sb, ConnectionMetadata.getInstance(ch));
               }
            });
            handleBulkResult(sb, handler.allocator());
            break;
         }
         case "TRACKING":
            ByteBufferUtils.stringToByteBufAscii("-ERR client tracking not supported\r\n", handler.allocator());
            break;
         case "TRACKINGINFO":
            ByteBufferUtils.stringToByteBufAscii("*0\r\n", handler.allocator());
            break;
      }
      return handler.myStage();
   }

   private void addInfo(StringBuilder sb, ConnectionMetadata metadata) {
      sb.append("id=");
      sb.append(metadata.id());
      sb.append(" addr=");
      sb.append(metadata.remoteAddress());
      sb.append(" laddr=");
      sb.append(metadata.localAddress());
      sb.append(" name=");
      String name = metadata.clientName();
      if (name != null) {
         sb.append(name);
      }
      sb.append(" age=");
      sb.append(Duration.between(metadata.created(), Instant.now()).getSeconds());
      sb.append(" user=");
      sb.append(Security.getSubjectUserPrincipalName(metadata.subject()));
      sb.append("\n");
   }
}
