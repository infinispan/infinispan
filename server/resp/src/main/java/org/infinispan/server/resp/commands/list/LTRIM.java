package org.infinispan.server.resp.commands.list;

import java.util.List;
import java.util.concurrent.CompletionStage;

import org.infinispan.multimap.impl.EmbeddedMultimapListCache;
import org.infinispan.server.resp.Consumers;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.ArgumentUtils;
import org.infinispan.server.resp.commands.Resp3Command;

import io.netty.channel.ChannelHandlerContext;

/**
 * @link https://redis.io/commands/ltrim/
 *
 * Trim an existing list so that it will contain only the specified range of elements specified.
 * Both start and stop are zero-based indexes, where 0 is the first element of the list (the head),
 * 1 the next element and so on.
 * start and end can also be negative numbers indicating offsets from the end of the list,
 * where -1 is the last element of the list, -2 the penultimate element and so on.
 * Out of range indexes will not produce an error:
 * if start is larger than the end of the list, or start > end,
 * the result will be an empty list (which causes key to be removed).
 * If end is larger than the end of the list, the command will treat it like the last element of the list.
 * Returns "OK"
 * @since 15.0
 */
public class LTRIM extends RespCommand implements Resp3Command {

   public LTRIM() {
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
      return handler.stageToReturn(listMultimap.trim(key, start, stop), ctx, Consumers.OK_BICONSUMER);
   }
}
