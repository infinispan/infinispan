package org.infinispan.server.resp.commands.list;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletionStage;

import org.infinispan.multimap.impl.EmbeddedMultimapListCache;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.ArgumentUtils;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.server.resp.serialization.Resp3Response;

import io.netty.channel.ChannelHandlerContext;

/**
 * LRANGE
 *
 * @see <a href="https://redis.io/commands/lrange/">LRANGE</a>
 * @since 15.0
 */
public class LRANGE extends RespCommand implements Resp3Command {

   public LRANGE() {
      super(4, 1, 1, 1);
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler,
                                                      ChannelHandlerContext ctx,
                                                      List<byte[]> arguments) {

      byte[] key = arguments.get(0);
      int start = ArgumentUtils.toInt(arguments.get(1));
      int stop = ArgumentUtils.toInt(arguments.get(2));

      EmbeddedMultimapListCache<byte[], byte[]> listMultimap = handler.getListMultimap();
      CompletionStage<Collection<byte[]>> cs = listMultimap.subList(key, start, stop)
            .thenApply(c -> c == null ? Collections.emptyList() : c);
      return handler.stageToReturn(cs, ctx, Resp3Response.ARRAY_BULK_STRING);
   }
}
