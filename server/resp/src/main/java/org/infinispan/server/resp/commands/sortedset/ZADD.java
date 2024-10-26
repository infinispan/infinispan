package org.infinispan.server.resp.commands.sortedset;

import static org.infinispan.multimap.impl.SortedSetAddArgs.ADD_AND_UPDATE_ONLY_INCOMPATIBLE_ERROR;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletionStage;

import org.infinispan.multimap.impl.EmbeddedMultimapSortedSetCache;
import org.infinispan.multimap.impl.ScoredValue;
import org.infinispan.multimap.impl.SortedSetAddArgs;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespErrorUtil;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.ArgumentUtils;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.server.resp.serialization.Resp3Response;

import io.netty.channel.ChannelHandlerContext;

/**
 * ZADD
 *
 * @see <a href="https://redis.io/commands/zadd/">ZADD</a>
 * @since 15.0
 */
public class ZADD extends RespCommand implements Resp3Command {

   public static final String XX = "XX";
   public static final String NX = "NX";
   public static final String LT = "LT";
   public static final String GT = "GT";
   public static final String CH = "CH";
   public static final String INCR = "INCR";

   public static final Set<String> ARGUMENTS = Set.of(XX, NX, LT, GT, CH, INCR);

   public ZADD() {
      super(-4, 1, 1, 1);
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler,
                                                      ChannelHandlerContext ctx,
                                                      List<byte[]> arguments) {

      //zadd key [NX|XX] [GT|LT] [CH] [INCR] score member [score member ...]
      byte[] name = arguments.get(0);
      SortedSetAddArgs.Builder addManyArgs = SortedSetAddArgs.create();
      EmbeddedMultimapSortedSetCache<byte[], byte[]> sortedSetCache = handler.getSortedSeMultimap();

      int pos = 1;
      while (pos < arguments.size()) {
         String arg = (new String(arguments.get(pos)).toUpperCase());
         if (ARGUMENTS.contains(arg)) {
            parseArgument(addManyArgs, arg);
            pos++;
         } else {
            break;
         }
      }

      // Validate arguments
      SortedSetAddArgs sortedSetAddArgs;
      try {
         sortedSetAddArgs = addManyArgs.build();
      } catch (IllegalArgumentException ex) {
         if (ex.getMessage().equals(ADD_AND_UPDATE_ONLY_INCOMPATIBLE_ERROR)) {
            RespErrorUtil.customError("XX and NX options at the same time are not compatible", handler.allocator());
         } else {
            RespErrorUtil.customError("GT, LT, and/or NX options at the same time are not compatible", handler.allocator());
         }
         return handler.myStage();
      }

      // Validate scores and values in pairs. We need at least 1 pair
      if (((arguments.size() - pos) == 0) || (arguments.size() - pos) % 2 != 0) {
         // Scores and Values come in pairs
         RespErrorUtil.syntaxError(handler.allocator());
         return handler.myStage();
      }

      int count = (arguments.size() - pos) / 2;
      if (sortedSetAddArgs.incr && count > 1) {
         RespErrorUtil.customError("INCR option supports a single increment-element pair", handler.allocator());
         return handler.myStage();
      }

      List<ScoredValue<byte[]>> scoredValues = new ArrayList<>(count);
      while (pos < arguments.size()) {
         double score;
         try {
            score = ArgumentUtils.toDouble(arguments.get(pos++));
         } catch (NumberFormatException e) {
            // validate number format
            RespErrorUtil.valueNotAValidFloat(handler.allocator());
            return handler.myStage();
         }
         byte[] value = arguments.get(pos++);
         scoredValues.add(ScoredValue.of(score, value));
      }

      if (sortedSetAddArgs.incr) {
         return handler.stageToReturn(sortedSetCache.incrementScore(name, scoredValues.get(0).score(), scoredValues.get(0).getValue(), sortedSetAddArgs),
               ctx, Resp3Response.DOUBLE);
      }

      return handler.stageToReturn(sortedSetCache.addMany(name, scoredValues, sortedSetAddArgs), ctx, Resp3Response.INTEGER);
   }

   private void parseArgument(SortedSetAddArgs.Builder addManyArgs, String argument) {
      switch (argument) {
         case NX:
            addManyArgs.addOnly();
            break;
         case XX:
            addManyArgs.updateOnly();
            break;
         case GT:
            addManyArgs.updateGreaterScoresOnly();
            break;
         case LT:
            addManyArgs.updateLessScoresOnly();
            break;
         case CH:
            addManyArgs.returnChangedCount();
            break;
         case INCR:
            addManyArgs.incr();
            break;
         default:
      }
   }
}
