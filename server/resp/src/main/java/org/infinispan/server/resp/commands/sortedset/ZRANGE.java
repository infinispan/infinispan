package org.infinispan.server.resp.commands.sortedset;

import io.netty.channel.ChannelHandlerContext;
import org.infinispan.multimap.impl.EmbeddedMultimapSortedSetCache;
import org.infinispan.multimap.impl.SortedSetAddArgs;
import org.infinispan.multimap.impl.SortedSetBucket;
import org.infinispan.multimap.impl.SortedSetSubsetArgs;
import org.infinispan.server.resp.Consumers;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespErrorUtil;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.ArgumentUtils;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.util.concurrent.CompletionStages;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletionStage;

import static org.infinispan.server.resp.commands.sortedset.ZSetCommonUtils.mapResultsToArrayList;

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
   public enum Arg {
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
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler,
                                                      ChannelHandlerContext ctx,
                                                      List<byte[]> arguments) {
      int pos = 0;
      final byte[] destination;
      if (this.getArity() == -5) {
         destination = arguments.get(pos++);
      } else {
         destination = null;
      }
      byte[] name = arguments.get(pos++);
      byte[] start = arguments.get(pos++);
      byte[] stop = arguments.get(pos++);
      final ResultOptions resultOpt = new ResultOptions();
      boolean isRev = this.initialRev;
      boolean byLex = this.initialByLex;
      boolean byScore = this.initialByScore;

      while (pos < arguments.size()) {
         switch (Arg.valueOf(new String(arguments.get(pos++)))) {
            case LIMIT:
               try {
                  resultOpt.offset = ArgumentUtils.toLong(arguments.get(pos++));
                  resultOpt.count = ArgumentUtils.toLong(arguments.get(pos++));
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
      }

      EmbeddedMultimapSortedSetCache<byte[], byte[]> sortedSetCache = handler.getSortedSeMultimap();

      // Syntax error when byLex and byScore
      if (byLex && byScore) {
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

      CompletionStage<Collection<SortedSetBucket.ScoredValue<byte[]>>> getSortedSetCall;
      if (byScore) {
         // ZRANGE by score. Replaces ZRANGEBYSCORE and ZREVRANGEBYSCORE
         ZSetCommonUtils.Score startScore = ZSetCommonUtils.parseScore(start);
         ZSetCommonUtils.Score stopScore = ZSetCommonUtils.parseScore(stop);
         if (startScore == null || stopScore == null) {
            RespErrorUtil.minOrMaxNotAValidFloat(handler.allocator());
            return handler.myStage();
         }

         SortedSetSubsetArgs.Builder<Double> builder = SortedSetSubsetArgs.create();
         builder.isRev(isRev);
         builder.start(startScore.value);
         builder.includeStart(startScore.include);
         builder.stop(stopScore.value);
         builder.includeStop(stopScore.include);
         builder.offset(resultOpt.offset);
         builder.count(resultOpt.count);
         getSortedSetCall = sortedSetCache.subsetByScore(name, builder.build());

      } else if (byLex) {
         // ZRANGE by lex. Replaces ZRANGEBYLEX and ZREVRANGEBYLEX
         ZSetCommonUtils.Lex startLex = ZSetCommonUtils.parseLex(start);
         ZSetCommonUtils.Lex stopLex = ZSetCommonUtils.parseLex(stop);
         if (startLex == null || stopLex == null) {
            RespErrorUtil.minOrMaxNotAValidStringRange(handler.allocator());
            return handler.myStage();
         }
         SortedSetSubsetArgs.Builder<byte[]> builder = SortedSetSubsetArgs.create();
         builder.isRev(isRev);
         builder.start(startLex.value);
         builder.includeStart(startLex.include);
         builder.stop(stopLex.value);
         builder.includeStop(stopLex.include);
         builder.offset(resultOpt.offset);
         builder.count(resultOpt.count);
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
      if (destination != null) {
         // Store range and return size
         return rangeAndStore(handler, ctx, destination, sortedSetCache, getSortedSetCall);
      }

      CompletionStage<List<byte[]>> list = getSortedSetCall.thenApply(subsetResult -> mapResultsToArrayList(subsetResult, resultOpt.withScores));
      return handler.stageToReturn(list, ctx, Consumers.GET_ARRAY_BICONSUMER);
   }

   private CompletionStage<RespRequestHandler> rangeAndStore(Resp3Handler handler,
                                                            ChannelHandlerContext ctx,
                                                            byte[] destination,
                                                            EmbeddedMultimapSortedSetCache<byte[], byte[]> sortedSetCache,
                                                            CompletionStage<Collection<SortedSetBucket.ScoredValue<byte[]>>> getSortedSetCall) {
      return CompletionStages.handleAndCompose(getSortedSetCall, (scoredValues, t1) -> {
         if (t1 != null) {
            return handleException(handler, t1);
         }

         CompletionStage<Long> sortedSetSize = sortedSetCache.addMany(destination, scoredValues, SortedSetAddArgs.create().replace().build());
         return handler.stageToReturn(sortedSetSize, ctx, Consumers.LONG_BICONSUMER);
      });
   }
}
