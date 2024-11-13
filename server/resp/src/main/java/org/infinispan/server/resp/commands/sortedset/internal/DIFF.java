package org.infinispan.server.resp.commands.sortedset.internal;

import static org.infinispan.server.resp.commands.sortedset.ZSetCommonUtils.isWithScoresArg;
import static org.infinispan.server.resp.commands.sortedset.ZSetCommonUtils.response;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import org.infinispan.commons.util.concurrent.CompletionStages;
import org.infinispan.multimap.impl.EmbeddedMultimapSortedSetCache;
import org.infinispan.multimap.impl.ScoredValue;
import org.infinispan.multimap.impl.SortedSetAddArgs;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.ArgumentUtils;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.server.resp.commands.sortedset.ZSetCommonUtils;
import org.infinispan.server.resp.serialization.ResponseWriter;

import io.netty.channel.ChannelHandlerContext;

/**
 * Common implementation for ZDIFF commands
 */
public abstract class DIFF extends RespCommand implements Resp3Command {
   private static final BiConsumer<Object, ResponseWriter> SERIALIZER = (res, writer) -> {
      if (res instanceof Long l) {
         writer.integers(l);
         return;
      }

      ZSetCommonUtils.ZOperationResponse zres = (ZSetCommonUtils.ZOperationResponse) res;
      writer.write(zres, zres);
   };

   protected DIFF(int arity, int firstKeyPos, int lastKeyPos, int steps) {
      super(arity, firstKeyPos, lastKeyPos, steps);
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler,
                                                      ChannelHandlerContext ctx,
                                                      List<byte[]> arguments) {
      int pos = 0;
      final byte[] destination;
      if (getArity() == -4) {
         destination = arguments.get(pos++);
      } else {
         destination = null;
      }

      int numberOfKeysArg;
      try {
         numberOfKeysArg = ArgumentUtils.toInt(arguments.get(pos++));
      } catch (NumberFormatException ex) {
         handler.writer().valueNotInteger();
         return handler.myStage();
      }

      if (numberOfKeysArg <= 0) {
         handler.writer().customError("at least 1 input key is needed for '" + this.getName().toLowerCase() + "' command");
         return handler.myStage();
      }

      final boolean withScores;
      if (getArity() == -3) {
         withScores = isWithScoresArg(arguments.get(arguments.size() - 1));
      } else {
         withScores = false;
      }

      if (invalidNumberOfKeys(arguments, numberOfKeysArg, withScores)) {
         handler.writer().syntaxError();
         return handler.myStage();
      }

      EmbeddedMultimapSortedSetCache<byte[], byte[]> sortedSetCache = handler.getSortedSeMultimap();
      CompletionStage<List<ScoredValue<byte[]>>> diffScoreValues = sortedSetCache.getValueAsList(arguments.get(pos++));
      for (int i = 1; i < numberOfKeysArg; i++) {
         final byte[] setName = arguments.get(pos++);
         diffScoreValues = CompletionStages.handleAndCompose(
               diffScoreValues, (c1, t1) -> {
                  if (t1 != null) {
                     return CompletableFuture.failedFuture(t1);
                  }
                  if (c1 == null || c1.isEmpty()) {
                     return CompletableFuture.completedFuture(Collections.emptyList());
                  }

                  return sortedSetCache.getValuesSet(setName).handle((c2, t2) -> {
                     if (c2 == null || c2.isEmpty()) {
                        return c1;
                     }
                     return c1.stream().filter(e -> !c2.contains(e.wrappedValue())).collect(Collectors.toList());
                  });
               });
      }

      CompletionStage<?> cs = diffScoreValues
            .thenCompose(result -> (CompletionStage<?>)
                  (destination != null
                        ? sortedSetCache.addMany(destination, result, SortedSetAddArgs.create().replace().build())
                        : CompletableFuture.completedFuture(response(result, withScores))));

      return handler.stageToReturn(cs, ctx, SERIALIZER);
   }

   private boolean invalidNumberOfKeys(List<byte[]> arguments, int numberOfKeys, boolean withScores) {
      int excludeCount = getArity() == -4 || withScores ? 2 : 1;
      return arguments.size() - excludeCount != numberOfKeys;
   }
}
