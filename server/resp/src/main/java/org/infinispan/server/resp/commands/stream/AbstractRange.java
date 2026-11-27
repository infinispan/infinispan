package org.infinispan.server.resp.commands.stream;

import static org.infinispan.server.resp.commands.stream.StreamCommandUtils.matches;
import static org.infinispan.server.resp.commands.stream.StreamCommandUtils.parseRangeBound;
import static org.infinispan.server.resp.commands.stream.StreamCommandUtils.writeEntries;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CompletionStage;

import org.infinispan.server.resp.AclCategory;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.ArgumentUtils;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.server.resp.commands.stream.StreamCommandUtils.RangeBound;
import org.infinispan.server.resp.exception.RespCommandException;
import org.infinispan.server.resp.stream.StreamRangeFunction;

import io.netty.channel.ChannelHandlerContext;

/**
 * Base for {@code XRANGE} and {@code XREVRANGE}: retrieves the entries within the provided ID range.
 *
 * @see <a href="https://redis.io/commands/xrange/">XRANGE</a>
 * @see <a href="https://redis.io/commands/xrevrange/">XREVRANGE</a>
 * @since 16.4
 */
abstract class AbstractRange extends RespCommand implements Resp3Command {

   private static final byte[] COUNT = "COUNT".getBytes(StandardCharsets.US_ASCII);
   private static final byte[] LIMIT = "LIMIT".getBytes(StandardCharsets.US_ASCII);

   private final boolean reverse;

   AbstractRange(boolean reverse) {
      super(-4, 1, 1, 1, AclCategory.READ.mask() | AclCategory.STREAM.mask() | AclCategory.SLOW.mask());
      this.reverse = reverse;
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler, ChannelHandlerContext ctx,
         List<byte[]> arguments) {
      byte[] key = arguments.get(0);

      // XRANGE key start end [COUNT count] | [LIMIT count]
      // XREVRANGE key end start [COUNT count] | [LIMIT count]
      // Both commands take the interval bounds with the higher bound first when reversed.
      RangeBound lower = parseRangeBound(arguments.get(reverse ? 2 : 1), true);
      RangeBound higher = parseRangeBound(arguments.get(reverse ? 1 : 2), false);

      long count = -1;
      boolean countGiven = false;
      int pos = 3;
      while (pos < arguments.size()) {
         byte[] argument = arguments.get(pos++);
         if (matches(argument, COUNT) || matches(argument, LIMIT)) {
            if (countGiven) {
               throw new RespCommandException("ERR syntax error, COUNT and LIMIT cannot be combined");
            }
            if (pos >= arguments.size()) {
               throw new RespCommandException("ERR syntax error");
            }
            count = ArgumentUtils.toLong(arguments.get(pos++));
            if (count < 0) {
               throw new RespCommandException("ERR The COUNT argument must be >= 0");
            }
            countGiven = true;
         } else {
            throw new RespCommandException("ERR syntax error");
         }
      }

      if (countGiven && count == 0) {
         // Following the Redis semantics, an explicit COUNT 0 replies with nil for an existing stream, and an empty
         // array for a missing key, instead of returning all the matching entries.
         return handler.stageToReturn(handler.getStreamCache().bucket(key), ctx, (bucket, writer) -> {
            if (bucket == null) {
               writeEntries(List.of(), writer);
            } else {
               writer.nulls();
            }
         });
      }

      StreamRangeFunction function = new StreamRangeFunction(lower.id(), lower.exclusive(),
            higher.id(), higher.exclusive(), reverse, count);
      return handler.stageToReturn(handler.getStreamCache().range(key, function), ctx,
            (entries, writer) -> writeEntries(entries, writer));
   }
}
