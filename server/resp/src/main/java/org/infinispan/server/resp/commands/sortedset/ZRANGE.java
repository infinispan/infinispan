package org.infinispan.server.resp.commands.sortedset;

import io.netty.channel.ChannelHandlerContext;
import org.infinispan.multimap.impl.EmbeddedMultimapSortedSetCache;
import org.infinispan.multimap.impl.SortedSetBucket;
import org.infinispan.multimap.impl.SortedSetSubsetArgs;
import org.infinispan.server.resp.Consumers;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespErrorUtil;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.ArgumentUtils;
import org.infinispan.server.resp.commands.Resp3Command;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletionStage;

/**
 * Returns the specified range of elements in the sorted set stored at <key>.
 * <p>
 * ZRANGE can perform different types of range queries: by index (rank), by the score,or by lexicographical order.
 * <p>
 * Starting with Redis 6.2.0, this command can replace the following commands:
 * <ul>
 *    <li>{@link ZREVRANGE}/li>
 *    <li>{@link ZRANGEBYSCORE}</li>
 *    <li>{@link ZREVRANGEBYSCORE}</li>
 *    <li>{@link ZRANGEBYLEX}</li>
 *    <li>{@link ZREVRANGEBYLEX}</li>
 * </ul>
 *
 * @see <a href="https://redis.io/commands/zrange">Redis Documentation</a>
 * @since 15.0
 */
public class ZRANGE extends RespCommand implements Resp3Command {
   protected enum Arg {
      BYSCORE, BYLEX, REV, LIMIT, WITHSCORES
   }

   private boolean initialRev;
   private boolean initialByScore;
   private boolean initialByLex;

   public ZRANGE() {
      this(-4, Collections.emptySet());
   }

   protected ZRANGE(int arity, Set<Arg> args) {
      this(arity);
      this.initialRev = args.contains(Arg.REV);
      this.initialByLex = args.contains(Arg.BYLEX);
      this.initialByScore = args.contains(Arg.BYSCORE);
   }

   protected ZRANGE(Set<Arg> args) {
      this(-4, args);
   }

   protected ZRANGE(int arity) {
      super(arity, 1, 1, 1);
   }

   static class ResultOptions {
      boolean withScores = false;
      Long offset = null;
      Long count = null;

      boolean limit() {
         return offset != null && count != null;
      }
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler, ChannelHandlerContext ctx,
                                                                List<byte[]> arguments) {
      byte[] name = arguments.get(0);
      byte[] start = arguments.get(1);
      byte[] stop = arguments.get(2);

      int pos = 3;
      final ResultOptions resultOpt = new ResultOptions();
      boolean isRev = this.initialRev;
      boolean byLex = this.initialByLex;
      boolean byScore = this.initialByScore;

      while (pos < arguments.size()) {
         switch (Arg.valueOf(new String(arguments.get(pos)))) {
            case LIMIT:
               try {
                  resultOpt.offset = ArgumentUtils.toLong(arguments.get(++pos));
                  resultOpt.count = ArgumentUtils.toLong(arguments.get(++pos));
               } catch (NumberFormatException ex) {
                  RespErrorUtil.valueNotInteger(handler.allocator());
                  return handler.myStage();
               } catch (IndexOutOfBoundsException ex) {
                  RespErrorUtil.syntaxError(handler.allocator());
                  return handler.myStage();
               }
               break;
            case BYSCORE:
               byScore = true;
               break;
            case BYLEX:
               byLex = true;
               break;
            case REV:
               isRev = true;
               break;
            case WITHSCORES:
               resultOpt.withScores = true;
               break;
            default:
               RespErrorUtil.syntaxError(handler.allocator());
               return handler.myStage();
         }
         pos++;
      }

      EmbeddedMultimapSortedSetCache<byte[], byte[]> sortedSetCache = handler.getSortedSeMultimap();

      // Syntax error when byLex and byScore
      if ((byLex && byScore)) {
         RespErrorUtil.syntaxError(handler.allocator());
         return handler.myStage();
      }

      // LIMIT use is for byLex and byScore
      if (!byLex && !byScore && resultOpt.offset != null) {
         RespErrorUtil.customError("syntax error, LIMIT is only supported in combination with either BYSCORE or BYLEX",
               handler.allocator());
         return handler.myStage();
      }

      // WITHSCORES use is for index range and byScore
      if (byLex && resultOpt.withScores) {
         RespErrorUtil.customError("syntax error, WITHSCORES not supported in combination with BYLEX",
               handler.allocator());
         return handler.myStage();
      }

      if (resultOpt.count != null && resultOpt.count == 0) {
         // The call does not need to be done. Count is 0. Size is called to raise ERR type if needed
         return returnEmptyList(handler, ctx, name, sortedSetCache);
      }

      CompletionStage<Collection<SortedSetBucket.ScoredValue<byte[]>>> getSortedSetCall;
      if (byScore) {
         // ZRANGE by score. Replaces ZRANGEBYSCORE and ZREVRANGEBYSCORE
         SortedSetArgumentsUtils.Score startScore = SortedSetArgumentsUtils.parseScore(start);
         SortedSetArgumentsUtils.Score stopScore = SortedSetArgumentsUtils.parseScore(stop);
         if (startScore == null || stopScore == null) {
            RespErrorUtil.minOrMaxNotAValidFloat(handler.allocator());
            return handler.myStage();
         }

         if ((isRev && startScore.unboundedMin) || (!isRev && startScore.unboundedMax)
               || (startScore.unboundedMin && stopScore.unboundedMin)
               || (startScore.unboundedMax && stopScore.unboundedMax)) {
            // no call when +inf in min or rev with -inf is empty, or unboundedMin and unboundedMax have the same value
            return returnEmptyList(handler, ctx, name, sortedSetCache);
         }

         SortedSetSubsetArgs.Builder<Double> builder = SortedSetSubsetArgs.create();
         builder.isRev(isRev);
         builder.start(startScore.value);
         builder.includeStart(startScore.include);
         builder.stop(stopScore.value);
         builder.includeStop(stopScore.include);
         getSortedSetCall = sortedSetCache.subsetByScore(name, builder.build());

      } else if (byLex) {
         // ZRANGE by lex. Replaces ZRANGEBYLEX and ZREVRANGEBYLEX
         SortedSetArgumentsUtils.Lex startLex = SortedSetArgumentsUtils.parseLex(start);
         SortedSetArgumentsUtils.Lex stopLex = SortedSetArgumentsUtils.parseLex(stop);
         if (startLex == null || stopLex == null) {
            RespErrorUtil.customError("min or max not valid string range item", handler.allocator());
            return handler.myStage();
         }
         if ((isRev && startLex.unboundedMin) || (!isRev && startLex.unboundedMax)
               || (startLex.unboundedMin && stopLex.unboundedMin)
               || (startLex.unboundedMax && stopLex.unboundedMax)) {
            // no call when + in min or rev with - is empty, or when we get + + or - -
            return returnEmptyList(handler, ctx, name, sortedSetCache);
         }

         SortedSetSubsetArgs.Builder<byte[]> builder = SortedSetSubsetArgs.create();
         builder.isRev(isRev);
         builder.start(startLex.value);
         builder.includeStart(startLex.include);
         builder.stop(stopLex.value);
         builder.includeStop(stopLex.include);
         getSortedSetCall = sortedSetCache.subsetByLex(name, builder.build());
      } else {
         // ZRANGE by index. Replaces ZREVRANGE
         long from;
         long to;
         try {
            from = ArgumentUtils.toLong(start);
            to = ArgumentUtils.toLong(stop);
         } catch (NumberFormatException ex) {
            RespErrorUtil.valueNotInteger(handler.allocator());
            return handler.myStage();
         }
         SortedSetSubsetArgs.Builder<Long> builder = SortedSetSubsetArgs.create();
         builder.isRev(isRev);
         builder.start(from);
         builder.stop(to);
         getSortedSetCall = sortedSetCache.subsetByIndex(name, builder.build());
      }
      CompletionStage<List<byte[]>> list = getSortedSetCall.thenApply(subsetResult -> mapResultsToArrayList(subsetResult, resultOpt));
      return handler.stageToReturn(list, ctx, Consumers.GET_ARRAY_BICONSUMER);
   }

   private static CompletionStage<RespRequestHandler> returnEmptyList(Resp3Handler handler,
                                                                      ChannelHandlerContext ctx,
                                                                      byte[] name,
                                                                      EmbeddedMultimapSortedSetCache<byte[], byte[]> sortedSetCache) {
      // Size is called to raise ERR type if needed
      return handler.stageToReturn(sortedSetCache.size(name)
                  .thenApply(r -> Collections.emptyList()), ctx, Consumers.GET_ARRAY_BICONSUMER);
   }

   /**
    * Transforms the resulting collection depending on the zrank options
    * - return scores
    * - limit results
    * @param scoredValues, scoresValues retrieved
    * @param resultOpt, result operations options
    * @return byte[] list to be returned by the command
    */
   private static List<byte[]> mapResultsToArrayList(Collection<SortedSetBucket.ScoredValue<byte[]>> scoredValues, ResultOptions resultOpt) {
      List<byte[]> elements = new ArrayList<>();
      Iterator<SortedSetBucket.ScoredValue<byte[]>> ite = scoredValues.iterator();
      if (resultOpt.limit()) {
         // skip the offset
         int offset = 0;
         while (offset++ < resultOpt.offset && ite.hasNext()) {
            ite.next();
         }
      }
      if (resultOpt.limit() && resultOpt.count > 0) {
         int count = 0;
         while (count++ < resultOpt.count && ite.hasNext()) {
            addScoredValue(elements, ite.next(), resultOpt.withScores);
         }
      } else {
         while (ite.hasNext()) {
            addScoredValue(elements, ite.next(), resultOpt.withScores);
         }
      }
      return elements;
   }

   private static void addScoredValue(List<byte[]> elements, SortedSetBucket.ScoredValue<byte[]> item, boolean withScores) {
      elements.add(item.getValue());
      if (withScores) {
         elements.add(Double.toString(item.score()).getBytes(StandardCharsets.US_ASCII));
      }
   }

}
