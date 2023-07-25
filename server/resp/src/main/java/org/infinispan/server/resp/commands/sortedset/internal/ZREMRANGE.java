package org.infinispan.server.resp.commands.sortedset.internal;

import io.netty.channel.ChannelHandlerContext;
import org.infinispan.multimap.impl.EmbeddedMultimapSortedSetCache;
import org.infinispan.server.resp.Consumers;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespErrorUtil;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.ArgumentUtils;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.server.resp.commands.sortedset.ZSetCommonUtils;

import java.util.List;
import java.util.concurrent.CompletionStage;

/**
 * Common ZREMRANGE commands
 */
public abstract class ZREMRANGE extends RespCommand implements Resp3Command {
   private final ZREMRANGE.Type type;

   protected enum Type {
      RANK, SCORE, LEX
   }
   protected ZREMRANGE(int arity, ZREMRANGE.Type type) {
      super(arity, 1, 1, 1);
      this.type = type;
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler,
                                                      ChannelHandlerContext ctx,
                                                      List<byte[]> arguments) {

      EmbeddedMultimapSortedSetCache<byte[], byte[]> sortedSetCache = handler.getSortedSeMultimap();
      int pos = 0;
      byte[] name = arguments.get(pos++);
      byte[] start = arguments.get(pos++);
      byte[] stop = arguments.get(pos++);

      CompletionStage<Long> removeAllCall;
      if (type == Type.SCORE) {
         // parse start - stop as scores
         ZSetCommonUtils.Score startScore = ZSetCommonUtils.parseScore(start);
         ZSetCommonUtils.Score stopScore = ZSetCommonUtils.parseScore(stop);
         if (startScore == null || stopScore == null) {
            RespErrorUtil.minOrMaxNotAValidFloat(handler.allocator());
            return handler.myStage();
         }
         removeAllCall = sortedSetCache.removeAll(name, startScore.value, startScore.include, stopScore.value, stopScore.include);
      } else if (type == Type.LEX) {
         // parse start - stop as lexical
         ZSetCommonUtils.Lex startLex = ZSetCommonUtils.parseLex(start);
         ZSetCommonUtils.Lex stopLex = ZSetCommonUtils.parseLex(stop);
         if (startLex == null || stopLex == null) {
            RespErrorUtil.customError("min or max not valid string range item", handler.allocator());
            return handler.myStage();
         }
         removeAllCall = sortedSetCache.removeAll(name, startLex.value, startLex.include, stopLex.value, stopLex.include);
      } else {
         // parse start - stop as index
         long from;
         long to;
         try {
            from = ArgumentUtils.toLong(start);
            to = ArgumentUtils.toLong(stop);
         } catch (NumberFormatException ex) {
            RespErrorUtil.valueNotInteger(handler.allocator());
            return handler.myStage();
         }
         removeAllCall = sortedSetCache.removeAll(name, from, to);
      }

      return handler.stageToReturn(removeAllCall, ctx, Consumers.LONG_BICONSUMER);
   }
}
