package org.infinispan.server.resp.commands.sortedset;

import io.netty.channel.ChannelHandlerContext;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.Resp3Command;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletionStage;

/**
 * When all the elements in a sorted set are inserted with the same score,
 * in order to force lexicographical ordering, this command returns all the elements in the
 * sorted set at key with a value between max and min.
 *
 * Apart from the reversed ordering, ZREVRANGEBYLEX is similar to {@link ZRANGEBYLEX}.
 * As of Redis version 6.2.0, this command is regarded as deprecated.
 *
 * It can be replaced by {@link ZRANGE}.
 *
 * @see <a href="https://redis.io/commands/zrevrangebylex">Redis Documentation</a>
 * @since 15.0
 */
public class ZREVRANGEBYLEX extends ZRANGE implements Resp3Command {
   public ZREVRANGEBYLEX() {
      super(Set.of(Arg.BYLEX, Arg.REV));
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler,
                                                      ChannelHandlerContext ctx,
                                                      List<byte[]> arguments) {
      return super.perform(handler, ctx, arguments);
   }
}
