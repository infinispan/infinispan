package org.infinispan.server.resp.commands.list;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CompletionStage;

import org.infinispan.multimap.impl.EmbeddedMultimapListCache;
import org.infinispan.server.resp.Consumers;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.Resp3Command;

import io.netty.channel.ChannelHandlerContext;

/**
 * @link https://redis.io/commands/lindex/
 *
 * Returns the element at the given index in the list.
 * The index is zero-based, so 0 means the first element, 1 the second element and so on.
 * Negative indices can be used to designate elements starting at the tail of the list.
 * -1 means the last element.
 * When the value at key is not a list, an error is returned.
 *
 * @since 15.0
 */
public class LINDEX extends RespCommand implements Resp3Command {
   public LINDEX() {
      super(3, 1, 1, 1);
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler,
                                                      ChannelHandlerContext ctx,
                                                      List<byte[]> arguments) {
      byte[] key = arguments.get(0);
      final long index = Long.parseLong(new String(arguments.get(1), StandardCharsets.US_ASCII));

      EmbeddedMultimapListCache<byte[], byte[]> listMultimap = handler.getListMultimap();
      CompletionStage<byte[]> value = listMultimap.index(key, index);
      return handler.stageToReturn(value, ctx, Consumers.GET_BICONSUMER);
   }
}
