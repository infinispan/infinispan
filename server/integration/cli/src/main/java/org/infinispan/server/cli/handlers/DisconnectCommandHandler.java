package org.infinispan.server.cli.handlers;

import org.infinispan.server.cli.util.CliCommandBuffer;
import org.infinispan.server.cli.util.InfinispanUtil;
import org.jboss.as.cli.CommandContext;
import org.jboss.as.cli.CommandHandler;
import org.jboss.as.cli.CommandHandlerProvider;
import org.jboss.as.cli.CommandLineException;

/**
 * Handles the disconnect
 *
 * @author Tristan Tarrant
 * @since 9.0
 */
public class DisconnectCommandHandler extends NoArgumentsCliCommandHandler {

   public DisconnectCommandHandler(CliCommandBuffer buffer) {
      super(CacheCommand.DISCONNECT, buffer);
   }

   @Override
   public void handle(CommandContext ctx) throws CommandLineException {
      ctx.terminateSession();
   }

   @Override
   public boolean isAvailable(CommandContext ctx) {
      return ctx.getModelControllerClient() != null;
   }

   public static class Provider implements CommandHandlerProvider {

      @Override
      public CommandHandler createCommandHandler(CommandContext ctx) {
         return new DisconnectCommandHandler(CliCommandBuffer.INSTANCE);
      }

      @Override
      public boolean isTabComplete() {
         return true;
      }

      @Override
      public String[] getNames() {
         return new String[] { CacheCommand.DISCONNECT.getName() };
      }

   }
}