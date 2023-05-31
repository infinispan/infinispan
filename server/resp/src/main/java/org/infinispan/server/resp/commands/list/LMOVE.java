package org.infinispan.server.resp.commands.list;

import io.netty.channel.ChannelHandlerContext;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.multimap.impl.EmbeddedMultimapListCache;
import org.infinispan.server.resp.Consumers;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespErrorUtil;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.server.resp.logging.Log;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletionStage;

/**
 * @link https://redis.io/commands/lmove/
 * <p>
 * Atomically returns and removes the first/last element (head/tail depending on the wherefrom argument)
 * of the list stored at source, and pushes the element at the first/last element (head/tail depending on the whereto argument)
 * of the list stored at destination.
 * <p>
 * For example: consider source holding the list a,b,c, and destination holding the list x,y,z.
 * Executing LMOVE source destination RIGHT LEFT results in source holding a,b and destination holding c,x,y,z.
 * <p>
 * If source does not exist, the value nil is returned and no operation is performed.
 * If source and destination are the same, the operation is equivalent to removing the first/last element
 * from the list and pushing it as first/last element of the list,
 * so it can be considered as a list rotation command (or a no-op if wherefrom is the same
 * as whereto).
 * <p>
 * This command comes in place of the now deprecated RPOPLPUSH.
 * Doing LMOVE RIGHT LEFT is equivalent.
 * @since 15.0
 */
public class LMOVE extends RespCommand implements Resp3Command {
   public static final String LEFT = "LEFT";
   public static final String RIGHT = "RIGHT";

   public LMOVE(int arity) {
      super(arity, 1, 2, 1);
   }

   public LMOVE() {
      super(5, 1, 2, 1);
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
            RespErrorUtil.syntaxError(handler.allocator());
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
         return handler.stageToReturn(performedCall, ctx, Consumers.GET_BICONSUMER);
      }

      CompletionStage<Collection<byte[]>> pollCall;
      if (isSourceLeft) {
         pollCall = listMultimap.pollFirst(source, 1);
      } else {
         pollCall = listMultimap.pollLast(source, 1);
      }

      return pollCall.thenCompose( pollResult -> {
               if (pollResult == null) {
                  return handler.stageToReturn(CompletableFutures.completedNull(), ctx, Consumers.GET_BICONSUMER);
               }

               final byte[] element = pollResult.iterator().next();

               CompletionStage<Void> offerCall;
               if (isDestinationLeft) {
                  offerCall = listMultimap.offerFirst(destination, element);
               } else {
                  offerCall = listMultimap.offerLast(destination, element);
               }

               return handler.stageToReturn(offerCall.thenApply(r -> element), ctx, Consumers.GET_BICONSUMER);
            });
   }
}
