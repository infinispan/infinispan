package org.infinispan.server.cli.handlers;

import org.infinispan.server.cli.util.CliCommandBuffer;
import org.jboss.as.cli.CommandContext;
import org.jboss.as.cli.CommandHandler;
import org.jboss.as.cli.CommandHandlerProvider;

/**
 * The {@link CacheCommand#GRANT} handler.
 *
 * @author Tristan Tarrant
 * @since 6.1
 */
public class GrantCommandHandler extends RoleManipulationCommandHandler {

   public GrantCommandHandler(CliCommandBuffer buffer) {
      super(CacheCommand.GRANT, buffer);
   }

   public static class Provider implements CommandHandlerProvider {

      @Override
      public CommandHandler createCommandHandler(CommandContext ctx) {
         return new GrantCommandHandler(CliCommandBuffer.INSTANCE);
      }

      @Override
      public boolean isTabComplete() {
         return true;
      }

      @Override
      public String[] getNames() {
         return new String[] { CacheCommand.GRANT.getName() };
      }

   }
}
