package org.infinispan.server.resp.commands.list;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletionStage;

import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.multimap.impl.EmbeddedMultimapListCache;
import org.infinispan.server.resp.AclCategory;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.server.resp.logging.Log;
import org.infinispan.server.resp.serialization.ResponseWriter;

import io.netty.channel.ChannelHandlerContext;

/**
 * LMOVE
 *
 * @see <a href="https://redis.io/commands/lmove/">LMOVE</a>
 * @since 15.0
 */
public class LMOVE extends RespCommand implements Resp3Command {
   public static final String LEFT = "LEFT";
   public static final String RIGHT = "RIGHT";

   public LMOVE(int arity, long aclMask) {
      super(arity, 1, 2, 1, aclMask);
   }

   public LMOVE() {
      this(5, AclCategory.WRITE.mask() | AclCategory.LIST.mask() | AclCategory.SLOW.mask());
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler, ChannelHandlerContext ctx,
                                                                List<byte[]> arguments) {

      return lmoveAndReturn(handler, ctx, arguments, false);
   }

   protected CompletionStage<RespRequestHandler> lmoveAndReturn(Resp3Handler handler,
                                                                ChannelHandlerContext ctx,
                                                                List<byte[]> arguments,
                                                                boolean rightLeft) {
      byte[] source = arguments.get(0);
      byte[] destination = arguments.get(1);

      boolean sameList = Arrays.equals(source, destination);

      if (!sameList) {
         // warn when different lists
         Log.SERVER.lmoveConsistencyMessage();
      }

      final boolean isSourceLeft;
      final boolean isDestinationLeft;

      if (rightLeft) {
         isSourceLeft = false;
         isDestinationLeft = true;
      } else {
         // parse and validate RIGHT and LEFT arguments
         final String sourceWhereFrom = new String(arguments.get(2)).toUpperCase();
         final String destinationWhereFrom = new String(arguments.get(3)).toUpperCase();
         isSourceLeft = LEFT.equals(sourceWhereFrom);
         isDestinationLeft = LEFT.equals(destinationWhereFrom);
         if ((!isSourceLeft && !RIGHT.equals(sourceWhereFrom)) || (!isDestinationLeft && !RIGHT.equals(
               destinationWhereFrom))) {
            handler.writer().syntaxError();
            return handler.myStage();
         }
      }

      EmbeddedMultimapListCache<byte[], byte[]> listMultimap = handler.getListMultimap();
      if (sameList) {
         CompletionStage<byte[]> performedCall;
         if (isSourceLeft && isDestinationLeft) {
            // peek first element and do nothing else
            performedCall = listMultimap.index(source, 0);
         } else if (!isSourceLeft && !isDestinationLeft) {
            // peek last element and do nothing else
            performedCall = listMultimap.index(source, -1);
         } else {
            // rotate from left (head->tail) to right or from right to left (tail->left)
            performedCall = listMultimap.rotate(source, isSourceLeft);
         }
         return handler.stageToReturn(performedCall, ctx, ResponseWriter.BULK_STRING_BYTES);
      }

      CompletionStage<Collection<byte[]>> pollCall;
      if (isSourceLeft) {
         pollCall = listMultimap.pollFirst(source, 1);
      } else {
         pollCall = listMultimap.pollLast(source, 1);
      }

      CompletionStage<byte[]> cs = pollCall
            .thenCompose(pollResult -> {
               if (pollResult == null) return CompletableFutures.completedNull();

               final byte[] element = pollResult.iterator().next();

               CompletionStage<Void> offerCall;
               if (isDestinationLeft) {
                  offerCall = listMultimap.offerFirst(destination, element);
               } else {
                  offerCall = listMultimap.offerLast(destination, element);
               }

               return offerCall.thenApply(r -> element);
            });

      return handler.stageToReturn(cs, ctx, ResponseWriter.BULK_STRING_BYTES);
   }
}
