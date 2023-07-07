package org.infinispan.server.resp.commands.hash;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

import org.infinispan.multimap.impl.EmbeddedMultimapPairCache;
import org.infinispan.server.resp.ByteBufferUtils;
import org.infinispan.server.resp.Consumers;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespErrorUtil;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.Util;
import org.infinispan.server.resp.commands.ArgumentUtils;
import org.infinispan.server.resp.commands.Resp3Command;

import io.netty.channel.ChannelHandlerContext;

/**
 * `<code>HRANDFIELD key [count [WITHVALUES]]</code>` command.
 * <p>
 * Returns <code>count</code> keys from the hash stored at <code>key</code>. Negative <code>count</code> can return
 * duplicated keys.
 *
 * @since 15.0
 * @see <a href="https://redis.io/commands/hrandfield/">Redis documentation</a>
 * @author José Bolina
 */
public class HRANDFIELD extends RespCommand implements Resp3Command {

   private static final byte[] WITH_VALUES = "WITHVALUES".getBytes(StandardCharsets.US_ASCII);

   public HRANDFIELD() {
      super(-2, 1, 1, 1);
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler, ChannelHandlerContext ctx, List<byte[]> arguments) {
      byte[] key = arguments.get(0);
      int count = 1;
      if (arguments.size() > 1) {
         count = ArgumentUtils.toInt(arguments.get(1));
      }

      if (count == 0) {
         ByteBufferUtils.bytesToResult(Collections.emptyList(), handler.allocator());
         return handler.myStage();
      }

      boolean withValues = false;
      if (arguments.size() > 2) {
         // Syntax error, only a single option acceptable.
         if (!Util.isAsciiBytesEquals(WITH_VALUES, arguments.get(2))) {
            RespErrorUtil.syntaxError(handler.allocator());
            return handler.myStage();
         }
         withValues = true;
      }

      EmbeddedMultimapPairCache<byte[], byte[], byte[]> multimap = handler.getHashMapMultimap();
      int finalCount = count;
      boolean finalWithValues = withValues;
      CompletionStage<Collection<byte[]>> cs = multimap.subSelect(key, Math.abs(count))
            .thenApply(e -> e == null ? null : readRandomFields(e, finalCount, finalWithValues));
      return handler.stageToReturn(cs, ctx, Consumers.GET_ARRAY_BICONSUMER);
   }

   private Collection<byte[]> readRandomFields(Map<byte[], byte[]> values, int count, boolean withValues) {
      if (count > 0) {
         return values.entrySet().stream()
               .map(entry -> withValues
                     ? List.of(entry.getKey(), entry.getValue())
                     : List.of(entry.getKey()))
               .flatMap(Collection::stream)
               .collect(Collectors.toList());
      }

      // If the count is negative, we need to return `abs(count)` elements, including duplicates.
      List<Map.Entry<byte[], byte[]>> entries = new ArrayList<>(values.entrySet());
      return ThreadLocalRandom.current().ints(0, entries.size())
            .limit(Math.abs(count))
            .mapToObj(i -> {
               var entry = entries.get(i);
               return withValues
                     ? List.of(entry.getKey(), entry.getValue())
                     : List.of(entry.getKey());
            })
            .flatMap(Collection::stream)
            .collect(Collectors.toList());
   }
}
