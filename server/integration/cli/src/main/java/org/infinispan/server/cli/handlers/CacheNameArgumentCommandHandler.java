package org.infinispan.server.cli.handlers;

import org.infinispan.server.cli.util.CliCommandBuffer;
import org.jboss.as.cli.CommandContext;
import org.jboss.as.cli.CommandHandler;
import org.jboss.as.cli.CommandHandlerProvider;
import org.jboss.as.cli.impl.ArgumentWithValue;

/**
 * The {@link CommandHandler} implementation for Infinispan CLI commands which have
 * the cache name as an argument.
 *
 * @author Pedro Ruivo
 * @since 6.1
 */
public class CacheNameArgumentCommandHandler extends NoArgumentsCliCommandHandler {

   protected final ArgumentWithValue cacheName;

   protected CacheNameArgumentCommandHandler(CacheCommand command, CliCommandBuffer buffer) {
      super(command, buffer);
      cacheName = new ArgumentWithValue(this, new CacheNameCommandCompleter(), 0, "--cache-name");
   }

   public static class BeginProvider implements CommandHandlerProvider {

      @Override
      public CommandHandler createCommandHandler(CommandContext ctx) {
         return new CacheNameArgumentCommandHandler(CacheCommand.BEGIN, CliCommandBuffer.INSTANCE);
      }

      @Override
      public boolean isTabComplete() {
         return true;
      }

      @Override
      public String[] getNames() {
         return new String[] { CacheCommand.BEGIN.getName() };
      }
   }

   public static class ClearProvider implements CommandHandlerProvider {

      @Override
      public CommandHandler createCommandHandler(CommandContext ctx) {
         return new CacheNameArgumentCommandHandler(CacheCommand.CLEARCACHE, CliCommandBuffer.INSTANCE);
      }

      @Override
      public boolean isTabComplete() {
         return true;
      }

      @Override
      public String[] getNames() {
         return new String[] { CacheCommand.CLEARCACHE.getName() };
      }

   }

   public static class InfoProvider implements CommandHandlerProvider {

      @Override
      public CommandHandler createCommandHandler(CommandContext ctx) {
         return new CacheNameArgumentCommandHandler(CacheCommand.INFO, CliCommandBuffer.INSTANCE);
      }

      @Override
      public boolean isTabComplete() {
         return true;
      }

      @Override
      public String[] getNames() {
         return new String[] { CacheCommand.INFO.getName() };
      }
   }

   public static class RollbackProvider implements CommandHandlerProvider {

      @Override
      public CommandHandler createCommandHandler(CommandContext ctx) {
         return new CacheNameArgumentCommandHandler(CacheCommand.ROLLBACK, CliCommandBuffer.INSTANCE);
      }

      @Override
      public boolean isTabComplete() {
         return true;
      }

      @Override
      public String[] getNames() {
         return new String[] { CacheCommand.ROLLBACK.getName() };
      }
   }

   public static class StartProvider implements CommandHandlerProvider {

      @Override
      public CommandHandler createCommandHandler(CommandContext ctx) {
         return new CacheNameArgumentCommandHandler(CacheCommand.START, CliCommandBuffer.INSTANCE);
      }

      @Override
      public boolean isTabComplete() {
         return true;
      }

      @Override
      public String[] getNames() {
         return new String[] { CacheCommand.START.getName() };
      }
   }
}
