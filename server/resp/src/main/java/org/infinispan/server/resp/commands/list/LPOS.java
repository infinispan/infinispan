package org.infinispan.server.resp.commands.list;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletionStage;

import org.infinispan.multimap.impl.EmbeddedMultimapListCache;
import org.infinispan.server.resp.AclCategory;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.ArgumentUtils;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.server.resp.serialization.Resp3Type;

import io.netty.channel.ChannelHandlerContext;

/**
 * LPOS
 *
 * @see <a href="https://redis.io/commands/lpos/">LPOS</a>
 * @since 15.0
 */
public class LPOS extends RespCommand implements Resp3Command {

   public static final String COUNT = "COUNT";
   public static final String RANK = "RANK";
   public static final String MAXLEN = "MAXLEN";

   public LPOS() {
      super(-3, 1, 1, 1);
   }

   @Override
   public long aclMask() {
      return AclCategory.READ | AclCategory.LIST | AclCategory.SLOW;
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler,
                                                      ChannelHandlerContext ctx,
                                                      List<byte[]> arguments) {

      if (arguments.size() % 2 != 0 || arguments.size() > 8) {
         //(error) (arguments must come in pairs)
         handler.writer().syntaxError();
         return handler.myStage();
      }

      CompletionStage<?> cs = lposAndReturn(handler, ctx, arguments);
      return handler.stageToReturn(cs, ctx, (res, writer) -> {
         if (res == handler) return;

         if (res == null || res instanceof Long) writer.integers((Number) res);
         else writer.array((Collection<?>) res, Resp3Type.INTEGER);
      });
   }

   private CompletionStage<?> lposAndReturn(Resp3Handler handler,
                                            ChannelHandlerContext ctx,
                                            List<byte[]> arguments) {
      byte[] key = arguments.get(0);
      byte[] element = arguments.get(1);
      Long count = null;
      Long rank = null;
      Long maxLen = null;

      for (int i = 2; i < arguments.size(); i++) {
         String argumentName = new String(arguments.get(i++)).toUpperCase();
         long argumentValue = ArgumentUtils.toLong(arguments.get(i));

         switch (argumentName) {
            case COUNT:
               count = argumentValue;
               if (count < 0) {
                  handler.writer().customError("COUNT can't be negative");
                  return handler.myStage();
               }
               break;
            case RANK:
               rank = argumentValue;
               if (rank == 0) {
                  handler.writer().customError("RANK can't be zero: use 1 to start from the first match, "
                        + "2 from the second ... or use negative to start from the end of the list");
                  return handler.myStage();
               }
               if (rank == Long.MIN_VALUE) {
                  handler.writer().customError("value is out of range, "
                        + "value must between -9223372036854775807 and 9223372036854775807");
                  return handler.myStage();
               }
               break;
            case MAXLEN:
               maxLen = argumentValue;
               if (maxLen < 0) {
                  handler.writer().customError("MAXLEN can't be negative");
                  return handler.myStage();
               }
               break;
            default:
               handler.writer().syntaxError();
               return handler.myStage();
         }
      }

      EmbeddedMultimapListCache<byte[], byte[]> listMultimap = handler.getListMultimap();
      final boolean returnSingleElement = count == null;
      return listMultimap.indexOf(key, element, count, rank, maxLen)
            .thenApply(indexes -> {
               if (returnSingleElement) {
                  return indexes == null || indexes.isEmpty() ? null : indexes.iterator().next();
               }

               return indexes == null ? Collections.emptyList() : indexes;
            });
   }

}
