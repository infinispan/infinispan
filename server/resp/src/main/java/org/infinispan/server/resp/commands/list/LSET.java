package org.infinispan.server.resp.commands.list;

import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.function.BiConsumer;

import org.infinispan.multimap.impl.EmbeddedMultimapListCache;
import org.infinispan.server.resp.ByteBufPool;
import org.infinispan.server.resp.Consumers;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespErrorUtil;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.ArgumentUtils;
import org.infinispan.server.resp.commands.Resp3Command;

import io.netty.channel.ChannelHandlerContext;

/**
 * @link https://redis.io/commands/lset/
 *
 * Sets the list element at index to element.
 * An error is returned for out of range indexes.
 *
 * @since 15.0
 */
public class LSET extends RespCommand implements Resp3Command {

   private static final BiConsumer<Boolean, ByteBufPool> RESPONSE_HANDLER = (result, buf) -> {
      if (!result) RespErrorUtil.noSuchKey(buf);
      else Consumers.OK_BICONSUMER.accept(null, buf);
   };

   public LSET() {
      super(4, 1, 1, 1);
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler,
                                                      ChannelHandlerContext ctx,
                                                      List<byte[]> arguments) {

      byte[] key = arguments.get(0);
      long index = ArgumentUtils.toLong(arguments.get(1));
      byte[] value = arguments.get(2);

      EmbeddedMultimapListCache<byte[], byte[]> listMultimap = handler.getListMultimap();
      return handler.stageToReturn(listMultimap.set(key, index, value), ctx, RESPONSE_HANDLER);
   }
}
