package org.infinispan.server.resp.commands.list;

import java.util.List;
import java.util.concurrent.CompletionStage;

import org.infinispan.multimap.impl.EmbeddedMultimapListCache;
import org.infinispan.server.resp.Consumers;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespErrorUtil;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.Resp3Command;

import io.netty.channel.ChannelHandlerContext;

/**
 * @link https://redis.io/commands/linsert/
 *
 * Inserts element in the list stored at key either before or after the reference value pivot.
 * When key does not exist, it is considered an empty list and no operation is performed.
 * An error is returned when key exists but does not hold a list value.
 * Returns the list length after a successful insert operation,
 * 0 if the key doesn't exist, and -1 when the pivot wasn't found.
 *
 * @since 15.0
 */
public class LINSERT extends RespCommand implements Resp3Command {

   public static final String BEFORE = "BEFORE";
   public static final String AFTER = "AFTER";

   public LINSERT() {
      super(5, 1, 1, 1);
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler,
                                                      ChannelHandlerContext ctx,
                                                      List<byte[]> arguments) {

      byte[] key = arguments.get(0);
      String position = new String(arguments.get(1)).toUpperCase();
      boolean isBefore = position.equals(BEFORE);
      if (!isBefore && !position.equals(AFTER)) {
         RespErrorUtil.syntaxError(handler.allocator());
         return handler.myStage();
      }

      byte[] pivot = arguments.get(2);
      byte[] element = arguments.get(3);

      EmbeddedMultimapListCache<byte[], byte[]> listMultimap = handler.getListMultimap();
      return handler.stageToReturn(listMultimap.insert(key, isBefore, pivot, element), ctx, Consumers.LONG_BICONSUMER);
   }
}
