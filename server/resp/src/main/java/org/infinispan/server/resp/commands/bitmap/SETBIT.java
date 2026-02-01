package org.infinispan.server.resp.commands.bitmap;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.infinispan.functional.FunctionalMap;
import org.infinispan.server.resp.AclCategory;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.ArgumentUtils;
import org.infinispan.server.resp.commands.Resp3Command;

import io.netty.channel.ChannelHandlerContext;

/**
 * SETBIT
 *
 * @see <a href="https://redis.io/commands/setbit/">SETBIT</a>
 * @since 16.2
 */
public class SETBIT extends RespCommand implements Resp3Command {
   public SETBIT() {
      super(4, 1, 1, 1, AclCategory.WRITE.mask() | AclCategory.STRING.mask() | AclCategory.SLOW.mask());
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler,
                                                      ChannelHandlerContext ctx,
                                                      List<byte[]> arguments) {
      byte[] key = arguments.get(0);
      int offset = ArgumentUtils.toInt(arguments.get(1));
      byte value = arguments.get(2)[0];

      if (value != '0' && value != '1') {
         handler.writer().customError("bit is not an integer or out of range");
         return handler.myStage();
      }

      FunctionalMap.ReadWriteMap<byte[], byte[]> fMap = FunctionalMap.create(handler.cache()).toReadWriteMap();
      BitfieldOperation op = new BitfieldOperation(BitfieldOperation.Type.SET, offset, value - '0', false, 1, BitfieldOperation.Overflow.NONE);
      CompletableFuture<List<Long>> results = fMap.eval(key, new BitfieldFunction(Collections.singletonList(op)));
      return handler.stageToReturn(results, ctx, (r, w) -> w.integers(r.get(0)));
   }
}
