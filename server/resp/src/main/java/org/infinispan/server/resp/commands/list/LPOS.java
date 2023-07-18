package org.infinispan.server.resp.commands.list;

import io.netty.channel.ChannelHandlerContext;
import org.infinispan.multimap.impl.EmbeddedMultimapListCache;
import org.infinispan.server.resp.Consumers;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespErrorUtil;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.ArgumentUtils;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.util.concurrent.CompletionStages;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * @link https://redis.io/commands/lpos/
 *
 * Returns the element at the given index in the list.
 * The index is zero-based, so 0 means the first element, 1 the second element and so on.
 * Negative indices can be used to designate elements starting at the tail of the list.
 * -1 means the last element.
 * When the value at key is not a list, an error is returned.
 *
 * @since 15.0
 * @see <a href="https://redis.io/commands/lpos">Redis Documentation</a>
 */
public class LPOS extends RespCommand implements Resp3Command {

   public static final String COUNT = "COUNT";
   public static final String RANK = "RANK";
   public static final String MAXLEN = "MAXLEN";

   public LPOS() {
      super(-3, 1, 1, 1);
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler,
                                                      ChannelHandlerContext ctx,
                                                      List<byte[]> arguments) {

      if (arguments.size() % 2 != 0  || arguments.size() > 8) {
         //(error) (arguments must come in pairs)
         RespErrorUtil.syntaxError(handler.allocator());
         return handler.myStage();
      }

      return lposAndReturn(handler, ctx, arguments);
   }

   protected CompletionStage<RespRequestHandler> lposAndReturn(Resp3Handler handler,
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
                  RespErrorUtil.customError("COUNT can't be negative", handler.allocator());
                  return handler.myStage();
               }
               break;
            case RANK:
               rank = argumentValue;
               if (rank == 0) {
                  RespErrorUtil.customError("RANK can't be zero: use 1 to start from the first match, "
                        + "2 from the second ... or use negative to start from the end of the list", handler.allocator());
                  return handler.myStage();
               }
               break;
            case MAXLEN:
               maxLen = argumentValue;
               if (maxLen < 0) {
                  RespErrorUtil.customError("MAXLEN can't be negative", handler.allocator());
                  return handler.myStage();
               }
               break;
            default:
               RespErrorUtil.syntaxError(handler.allocator());
               return handler.myStage();
         }
      }

      EmbeddedMultimapListCache<byte[], byte[]> listMultimap = handler.getListMultimap();
      final boolean returnSingleElement = count == null;
      return CompletionStages.handleAndCompose(listMultimap.indexOf(key, element, count, rank, maxLen), (indexes, t) -> {
         if (t != null) {
            return handleException(handler, t);
         }

         if (returnSingleElement) {
            Long singleIndex = indexes == null || indexes.isEmpty() ? null : indexes.iterator().next();
            return handler.stageToReturn(CompletableFuture.completedFuture(singleIndex), ctx, Consumers.LONG_BICONSUMER);
         }

         return handler.stageToReturn(CompletableFuture.completedFuture(indexes == null ? Collections.emptyList() : indexes), ctx, Consumers.COLLECTION_LONG_BICONSUMER);
      });
   }

}
