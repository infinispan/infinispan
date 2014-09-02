package org.infinispan.server.cli.handlers;

import org.infinispan.server.cli.util.CliCommandBuffer;
import org.infinispan.server.cli.util.InfinispanUtil;
import org.jboss.as.cli.CommandContext;
import org.jboss.as.cli.CommandHandler;
import org.jboss.as.cli.CommandHandlerProvider;
import org.jboss.as.cli.CommandLineException;
import org.jboss.dmr.ModelNode;

/**
 * The {@link CacheCommand#CACHE} handler.
 *
 * @author Pedro Ruivo
 * @since 6.1
 */
public class CacheCommandHandler extends CacheNameArgumentCommandHandler {

   public CacheCommandHandler(CliCommandBuffer buffer) {
      super(CacheCommand.CACHE, buffer);
   }

   @Override
   protected void printResult(ModelNode result, CommandContext context) throws CommandLineException {
      InfinispanUtil.changeToCache(context, InfinispanUtil.getCacheInfo(context).getContainer(),
            cacheName.getValue(context.getParsedCommandLine()));
      super.printResult(result, context);
   }

   public static class Provider implements CommandHandlerProvider {

      @Override
      public CommandHandler createCommandHandler(CommandContext ctx) {
         return new CacheCommandHandler(CliCommandBuffer.INSTANCE);
      }

      @Override
      public boolean isTabComplete() {
         return true;
      }

      @Override
      public String[] getNames() {
         return new String[] { CacheCommand.CACHE.getName() };
      }

   }
}
