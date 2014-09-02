package org.infinispan.server.cli.handlers;

import org.infinispan.server.cli.util.CliCommandBuffer;
import org.jboss.as.cli.CommandContext;
import org.jboss.as.cli.CommandHandler;
import org.jboss.as.cli.CommandHandlerProvider;
import org.jboss.as.cli.impl.ArgumentWithValue;
import org.jboss.as.cli.impl.ArgumentWithoutValue;

/**
 * The {@link CacheCommand#ENCODING} handler.
 *
 * @author Pedro Ruivo
 * @since 6.1
 */
public class EncodingCommandHandler extends NoArgumentsCliCommandHandler {

   public EncodingCommandHandler(CliCommandBuffer buffer) {
      super(CacheCommand.ENCODING, buffer);
      new ArgumentWithValue(this, null, 0, "--codec");
      new ArgumentWithoutValue(this, "--list");
   }

   public static class Provider implements CommandHandlerProvider {

      @Override
      public CommandHandler createCommandHandler(CommandContext ctx) {
         return new EncodingCommandHandler(CliCommandBuffer.INSTANCE);
      }

      @Override
      public boolean isTabComplete() {
         return true;
      }

      @Override
      public String[] getNames() {
         return new String[] { CacheCommand.ENCODING.getName() };
      }

   }
}
