package org.infinispan.server.cli.handlers;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.infinispan.server.cli.util.CliCommandBuffer;
import org.jboss.as.cli.CommandArgument;
import org.jboss.as.cli.CommandContext;
import org.jboss.as.cli.CommandFormatException;
import org.jboss.as.cli.CommandHandler;
import org.jboss.as.cli.CommandHandlerProvider;
import org.jboss.as.cli.handlers.SimpleTabCompleter;
import org.jboss.as.cli.impl.ArgumentWithValue;
import org.jboss.as.cli.operation.ParsedCommandLine;

/**
 * The {@link CacheCommand#REPLACE} handler.
 *
 * @author Pedro Ruivo
 * @since 6.0
 */
public class ReplaceCommandHandler extends KeyWithCodecCommandHandler {

   private final ArgumentWithValue expires;
   private final ArgumentWithValue maxIdle;

   public ReplaceCommandHandler(CliCommandBuffer buffer) {
      super(CacheCommand.REPLACE, buffer);
      new ArgumentWithValue(this, null, 1, "--value");
      new ArgumentWithValue(this, null, 2, "--other-value");
      expires = new ArgumentWithValue(this, new SimpleTabCompleter(new String[] { "expires" }), 3, "--expires");
      new ArgumentWithValue(this, null, 4, "--expiration");
      maxIdle = new ArgumentWithValue(this, new SimpleTabCompleter(new String[] { "maxidle" }), 5, "--max-idle");
      new ArgumentWithValue(this, null, 6, "--max-idle-time");
   }

   @Override
   public Collection<CommandArgument> getArguments(CommandContext ctx) {
      List<CommandArgument> argumentList = new ArrayList<CommandArgument>(8);
      try {
         ParsedCommandLine parsedCommandLine = ctx.getParsedCommandLine();
         int size = parsedCommandLine.getOtherProperties().size();
         if (!codec.isPresent(parsedCommandLine) && size == 0) {
            argumentList.add(codec);
         }
         if (addIfMissing(parsedCommandLine, "expires", 2) || addIfMissing(parsedCommandLine, "expires", 3)) {
            argumentList.add(expires);
         }
         if (addIfMissing(parsedCommandLine, "maxidle", 4) || addIfMissing(parsedCommandLine, "maxidle", 5)) {
            argumentList.add(maxIdle);
         }
      } catch (CommandFormatException e) {
         //ignored!
      }
      return argumentList;
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
         return new ReplaceCommandHandler(CliCommandBuffer.INSTANCE);
      }

      @Override
      public boolean isTabComplete() {
         return true;
      }

      @Override
      public String[] getNames() {
         return new String[] { CacheCommand.REPLACE.getName() };
      }

   }
}
