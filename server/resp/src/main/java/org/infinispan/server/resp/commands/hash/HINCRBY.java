package org.infinispan.server.resp.commands.hash;

import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicBoolean;

import org.infinispan.multimap.impl.EmbeddedMultimapPairCache;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespErrorUtil;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.ArgumentUtils;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.server.resp.serialization.Resp3Response;

import io.netty.channel.ChannelHandlerContext;

/**
 * `<code>HINCRBY key field increment</code>` command.
 * <p>
 * Increments the long value at the <code>field</code> in the hash map stored at <code>key</code> by <code>increment</code>.
 * The command returns an error if the stored value is not a numeric long.
 *
 * @since 15.0
 * @see <a href="https://redis.io/commands/hincrby/">Redis Documentation</a>
 * @author José Bolina
 */
public class HINCRBY extends RespCommand implements Resp3Command {

   public HINCRBY() {
      super(4, 1, 1, 1);
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler, ChannelHandlerContext ctx, List<byte[]> arguments) {
      EmbeddedMultimapPairCache<byte[], byte[], byte[]> multimap = handler.getHashMapMultimap();
      long delta = ArgumentUtils.toLong(arguments.get(2));
      AtomicBoolean failed = new AtomicBoolean(false);
      CompletionStage<byte[]> cs = multimap.compute(arguments.get(0), arguments.get(1), (ignore, prev) -> {
         if (prev == null) return arguments.get(2);

         // This is supposed to throw when the `prev` is not a long.
         long prevLong = ArgumentUtils.toLong(prev);
         long result = prevLong + delta;

         // Same check as Math#addExact(long, long).
         // We bring it here to avoid throwing an exception.
         if (((prevLong ^ result) & (delta ^ result)) < 0) {
            failed.set(true);
            return prev;
         }
         return ArgumentUtils.toByteArray(result);
      });
      return handler.stageToReturn(cs, ctx, (res, alloc) -> {
         if (failed.get()) {
            RespErrorUtil.customError("increment or decrement would overflow", alloc);
         } else {
            Resp3Response.integers(ArgumentUtils.toLong(res), alloc);
         }
      });
   }
}
