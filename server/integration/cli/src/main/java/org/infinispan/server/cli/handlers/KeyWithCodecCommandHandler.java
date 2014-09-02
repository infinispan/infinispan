package org.infinispan.server.cli.handlers;

import org.infinispan.server.cli.util.CliCommandBuffer;
import org.jboss.as.cli.CommandContext;
import org.jboss.as.cli.CommandHandler;
import org.jboss.as.cli.CommandHandlerProvider;
import org.jboss.as.cli.impl.ArgumentWithValue;

/**
 * The {@link CommandHandler} implementation which has a key and the codec as
 * arguments.
 *
 * @author Pedro Ruivo
 * @since 6.0
 */
public class KeyWithCodecCommandHandler extends KeyCommandHandler {

   protected final ArgumentWithValue codec;

   public KeyWithCodecCommandHandler(CacheCommand cacheCommand, CliCommandBuffer buffer) {
      super(cacheCommand, buffer);
      this.codec = new ArgumentWithValue(this, null, -1, "--codec");
   }

   public static class GetProvider implements CommandHandlerProvider {

      @Override
      public CommandHandler createCommandHandler(CommandContext ctx) {
         return new CacheNameArgumentCommandHandler(CacheCommand.GET, CliCommandBuffer.INSTANCE);
      }

      @Override
      public boolean isTabComplete() {
         return true;
      }

      @Override
      public String[] getNames() {
         return new String[] { CacheCommand.GET.getName() };
      }

   }
}
