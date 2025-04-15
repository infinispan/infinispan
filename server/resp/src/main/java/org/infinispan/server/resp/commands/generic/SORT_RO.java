package org.infinispan.server.resp.commands.generic;

import java.util.List;
import java.util.concurrent.CompletionStage;

import org.infinispan.server.resp.AclCategory;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.Resp3Command;

import io.netty.channel.ChannelHandlerContext;

/**
 * SORT_RO
 *
 * @see <a href="https://redis.io/commands/sort_ro/">SORT_RO</a>
 * @since 15.0
 */
public class SORT_RO extends RespCommand implements Resp3Command {
   private final SORT sort;
   public SORT_RO() {
      super(-2, 1, 1, 1, AclCategory.READ.mask() | AclCategory.SET.mask() | AclCategory.SORTEDSET.mask() | AclCategory.LIST.mask() | AclCategory.SLOW.mask() | AclCategory.DANGEROUS.mask());
      sort = new SORT();
      sort.disableStore();
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler,
                                                      ChannelHandlerContext ctx,
                                                      List<byte[]> arguments) {

      return sort.perform(handler, ctx, arguments);
   }

}
