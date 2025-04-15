package org.infinispan.server.resp.commands.hash;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import org.infinispan.multimap.impl.EmbeddedMultimapPairCache;
import org.infinispan.server.resp.AclCategory;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.RespUtil;
import org.infinispan.server.resp.commands.ArgumentUtils;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.server.resp.serialization.Resp3Type;
import org.infinispan.server.resp.serialization.ResponseWriter;

import io.netty.channel.ChannelHandlerContext;

/**
 * HRANDFIELD
 *
 * @author José Bolina
 * @see <a href="https://redis.io/commands/hrandfield/">HRANDFIELD</a>
 * @since 15.0
 */
public class HRANDFIELD extends RespCommand implements Resp3Command {

   private static final byte[] WITH_VALUES = "WITHVALUES".getBytes(StandardCharsets.US_ASCII);

   public HRANDFIELD() {
      super(-2, 1, 1, 1, AclCategory.READ.mask() | AclCategory.HASH.mask() | AclCategory.SLOW.mask());
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler, ChannelHandlerContext ctx, List<byte[]> arguments) {
      byte[] key = arguments.get(0);
      int count = 1;
      boolean countDefined = false;
      if (arguments.size() > 1) {
         count = ArgumentUtils.toInt(arguments.get(1));
         countDefined = true;
      }

      if (count == 0) {
         handler.writer().arrayEmpty();
         return handler.myStage();
      }

      boolean withValues = false;
      if (arguments.size() > 2) {
         // Syntax error, only a single option acceptable.
         if (!RespUtil.isAsciiBytesEquals(WITH_VALUES, arguments.get(2))) {
            handler.writer().syntaxError();
            return handler.myStage();
         }
         withValues = true;
      }

      EmbeddedMultimapPairCache<byte[], byte[], byte[]> multimap = handler.getHashMapMultimap();
      BiConsumer<Map<byte[], byte[]>, ResponseWriter> consumer = consumeResponse(count, withValues, countDefined);
      CompletionStage<Map<byte[], byte[]>> cs = multimap.subSelect(key, Math.abs(count));
      return handler.stageToReturn(cs, ctx, consumer);
   }

   private BiConsumer<Map<byte[], byte[]>, ResponseWriter> consumeResponse(int count, boolean withValues, boolean countDefined) {
      return (res, writer) -> {
         if (res == null) {
            // The key doesn't exist but the command has the COUNT argument, return an empty list.
            if (countDefined) writer.arrayEmpty();
               // Otherwise, simply return null.
            else writer.nulls();
            return;
         }

         Collection<Collection<byte[]>> parsed = readRandomFields(res, count, withValues);
         // When the number of entries is restricted, we return a list of elements.
         if (countDefined) {
            // In case the return contains the values, we return a list of lists.
            // Each sub-list has two elements, the key and the value.
            if (withValues) {
               writer.array(parsed, (c, a) -> a.array(c, Resp3Type.BULK_STRING));
               return;
            }
            writer.array(parsed.stream().flatMap(Collection::stream).toList(), Resp3Type.BULK_STRING);
            return;
         }

         // Otherwise, we return a bulk string with a single key.
         Collection<byte[]> bytes = parsed.iterator().next();
         writer.string(bytes.iterator().next());
      };
   }

   private Collection<Collection<byte[]>> readRandomFields(Map<byte[], byte[]> values, int count, boolean withValues) {
      if (count > 0) {
         return values.entrySet().stream()
               .map(entry -> withValues
                     ? List.of(entry.getKey(), entry.getValue())
                     : List.of(entry.getKey()))
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
            .collect(Collectors.toList());
   }
}
