package org.infinispan.server.resp.commands.sortedset;

import io.netty.channel.ChannelHandlerContext;
import org.infinispan.multimap.impl.EmbeddedMultimapSortedSetCache;
import org.infinispan.multimap.impl.SortedSetAddArgs;
import org.infinispan.multimap.impl.SortedSetBucket;
import org.infinispan.server.resp.Consumers;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespErrorUtil;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.ArgumentUtils;
import org.infinispan.server.resp.commands.Resp3Command;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletionStage;

import static org.infinispan.multimap.impl.SortedSetAddArgs.ADD_AND_UPDATE_ONLY_INCOMPATIBLE_ERROR;

/**
 * Adds all the specified members with the specified scores to the sorted set stored at key.
 * It is possible to specify multiple score / member pairs.
 * If a specified member is already a member of the sorted set,
 * the score is updated and the element reinserted at the right position to ensure the correct ordering.
 * <p>
 * If key does not exist, a new sorted set with the specified members as sole members is created,
 * like if the sorted set was empty.
 * If the key exists but does not hold a sorted set, an error is returned.
 * <p>
 * Options:
 * <ul>
 * <li>XX: Only update elements that already exist. Don't add new elements.</li>
 * <li>NX: Only add new elements. Don't update already existing elements.</li>
 * <li>LT: Only update existing elements if the new score is less than the current score. This flag doesn't prevent adding new elements.</li>
 * <li>GT: Only update existing elements if the new score is greater than the current score. This flag doesn't prevent adding new elements.</li>
 * <li>CH: Modify the return value from the number of new elements added, to the total number of elements changed.
 * Changed elements are new elements added and elements already existing for which the score was updated.
 * Normally the return value of ZADD only counts the number of new elements added.</li>
 * <li>INCR: When this option is specified ZADD acts like {@link ZINCRBY}.
 * Only one score-element pair can be specified in this mode.</li>
 * </ul>
 * Note: The GT, LT and NX options are mutually exclusive.
 * The score values should be the string representation of a double precision floating
 * point number. +inf and -inf values are valid values as well.
 *
 * @since 15.0
 * @see <a href="https://redis.io/commands/zadd">Redis Documentation</a>
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
         String arg = new String(arguments.get(pos));
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

      List<SortedSetBucket.ScoredValue<byte[]>> scoredValues = new ArrayList<>(count);
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
         scoredValues.add(SortedSetBucket.ScoredValue.of(score, value));
      }

      if (sortedSetAddArgs.incr) {
         return handler.stageToReturn(sortedSetCache.incrementScore(name, scoredValues.get(0).score(), scoredValues.get(0).getValue(), sortedSetAddArgs),
               ctx, Consumers.DOUBLE_BICONSUMER);
      }

      return handler.stageToReturn(sortedSetCache.addMany(name, scoredValues, sortedSetAddArgs),
            ctx, Consumers.LONG_BICONSUMER);
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
