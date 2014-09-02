package org.infinispan.server.cli.handlers;

import java.util.Collection;
import java.util.Collections;

import org.infinispan.server.cli.util.CliCommandBuffer;
import org.jboss.as.cli.CommandArgument;
import org.jboss.as.cli.CommandContext;
import org.jboss.as.cli.CommandHandler;
import org.jboss.as.cli.CommandHandlerProvider;
import org.jboss.as.cli.handlers.SimpleTabCompleter;
import org.jboss.as.cli.impl.ArgumentWithValue;
import org.jboss.as.cli.operation.ParsedCommandLine;

/**
 * The {@link CacheCommand#CREATE} handler.
 *
 * @author Pedro Ruivo
 * @since 6.1
 */
public class CreateCommandHandler extends NoArgumentsCliCommandHandler {

   private final ArgumentWithValue cacheName;
   private final ArgumentWithValue like;
   private final ArgumentWithValue baseCacheName;

   public CreateCommandHandler(CliCommandBuffer buffer) {
      super(CacheCommand.CREATE, buffer);
      cacheName = new ArgumentWithValue(this, null, 0, "--cache-name");
      like = new ArgumentWithValue(this, new SimpleTabCompleter(new String[] { "like" }), 1, "--like");
      baseCacheName = new ArgumentWithValue(this, new CacheNameCommandCompleter(), 2, "--base-cache-name");
   }

   @Override
   public Collection<CommandArgument> getArguments(CommandContext ctx) {
      ParsedCommandLine parsedCommandLine = ctx.getParsedCommandLine();
      if (parsedCommandLine.getOtherProperties().size() == 0) {
         return Collections.<CommandArgument> singleton(cacheName);
      } else if (addIfMissing(parsedCommandLine, "like", 1)) {
         return Collections.<CommandArgument> singleton(like);
      } else if (parsedCommandLine.getOtherProperties().size() == 2
            || (parsedCommandLine.getOtherProperties().size() == 3 && parsedCommandLine.getLastParsedPropertyValue() != null)) {
         return Collections.<CommandArgument> singleton(baseCacheName);
      }
      return Collections.emptyList();
   }

   private static boolean addIfMissing(ParsedCommandLine parsedCommandLine, String name, int index) {
      int size = parsedCommandLine.getOtherProperties().size();
      String lastProperty = parsedCommandLine.getLastParsedPropertyValue();
      return (size == index && lastProperty == null)
            || (size == index + 1 && lastProperty != null && name.startsWith(lastProperty));
   }

   public static class Provider implements CommandHandlerProvider {

      @Override
      public CommandHandler createCommandHandler(CommandContext ctx) {
         return new CreateCommandHandler(CliCommandBuffer.INSTANCE);
      }

      @Override
      public boolean isTabComplete() {
         return true;
      }

      @Override
      public String[] getNames() {
         return new String[] { CacheCommand.CREATE.getName() };
      }

   }
}
