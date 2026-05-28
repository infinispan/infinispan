package org.infinispan.server.resp.commands.cluster;

import java.util.List;
import java.util.concurrent.CompletionStage;

import org.infinispan.security.actions.SecurityActions;
import org.infinispan.server.resp.AclCategory;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.Resp3Command;

import io.netty.channel.ChannelHandlerContext;

/**
 * CLUSTER MYID
 * <p>
 * Returns the node's unique identifier. In Infinispan this corresponds to the cache manager's node name.
 * </p>
 *
 * @see <a href="https://redis.io/commands/cluster-myid/">CLUSTER MYID</a>
 * @since 16.2
 */
public class MYID extends RespCommand implements Resp3Command {

   public MYID() {
      super(2, 0, 0, 0, AclCategory.SLOW.mask());
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler, ChannelHandlerContext ctx, List<byte[]> arguments) {
      String nodeId = SecurityActions.getEmbeddedCacheManager(handler.cache()).getCacheManagerInfo().getNodeName();
      handler.writer().string(nodeId);
      return handler.myStage();
   }
}
