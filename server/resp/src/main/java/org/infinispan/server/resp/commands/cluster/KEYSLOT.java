package org.infinispan.server.resp.commands.cluster;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.infinispan.AdvancedCache;
import org.infinispan.distribution.ch.KeyPartitioner;
import org.infinispan.security.AuthorizationPermission;
import org.infinispan.security.actions.SecurityActions;
import org.infinispan.server.resp.Consumers;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.Resp3Command;

import io.netty.channel.ChannelHandlerContext;

/**
 * `<code>CLUSTER KEYSLOT key</code>` command.
 * <p>
 * Returns the slot a key is mapped to. Useful for debugging.
 *
 * @since 15.0
 * @see <a href="https://redis.io/commands/cluster-keyslot/">Redis Documentation</a>
 */
public class KEYSLOT extends RespCommand implements Resp3Command {

   public KEYSLOT() {
      super(3, 0, 0,0);
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler, ChannelHandlerContext ctx, List<byte[]> arguments) {
      handler.checkPermission(AuthorizationPermission.ADMIN);
      AdvancedCache<?, ?> respCache = handler.cache();

      KeyPartitioner partitioner = SecurityActions.getCacheComponentRegistry(respCache)
            .getComponent(KeyPartitioner.class);

      CompletionStage<Integer> cs = CompletableFuture.completedFuture(partitioner.getSegment(arguments.get(1)));
      return handler.stageToReturn(cs, ctx, Consumers.INT_BICONSUMER);
   }
}
