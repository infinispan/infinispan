package org.infinispan.server.cli.handlers;

import org.infinispan.server.cli.util.CliCommandBuffer;
import org.jboss.as.cli.CommandContext;
import org.jboss.as.cli.CommandHandler;
import org.jboss.as.cli.CommandHandlerProvider;
import org.jboss.as.cli.impl.ArgumentWithValue;

/**
 * The {@link CommandHandler} implementation which has a key as an argument.
 *
 * @author Pedro Ruivo
 * @since 6.1
 */
public class KeyCommandHandler extends NoArgumentsCliCommandHandler {

   protected final ArgumentWithValue key;

   public KeyCommandHandler(CacheCommand cacheCommand, CliCommandBuffer buffer) {
      this(cacheCommand, buffer, 0);
   }

   public KeyCommandHandler(CacheCommand cacheCommand, CliCommandBuffer buffer, int keyIndex) {
      super(cacheCommand, buffer);
      key = new ArgumentWithValue(this, null, keyIndex, "--key");
   }

   public static class EvictProvider implements CommandHandlerProvider {

      @Override
      public CommandHandler createCommandHandler(CommandContext ctx) {
         return new KeyCommandHandler(CacheCommand.EVICT, CliCommandBuffer.INSTANCE);
      }

      @Override
      public boolean isTabComplete() {
         return true;
      }

      @Override
      public String[] getNames() {
         return new String[] { CacheCommand.EVICT.getName() };
      }

   }
}
