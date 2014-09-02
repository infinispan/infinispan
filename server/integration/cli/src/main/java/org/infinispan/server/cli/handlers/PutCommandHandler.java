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
import org.jboss.as.cli.impl.ArgumentWithoutValue;
import org.jboss.as.cli.operation.ParsedCommandLine;

/**
 * The {@link CacheCommand#PUT} handler.
 *
 * @author Pedro Ruivo
 * @since 6.1
 */
public class PutCommandHandler extends KeyWithCodecCommandHandler {

   private final ArgumentWithoutValue putIfAbsent;
   private final ArgumentWithValue expires;
   private final ArgumentWithValue maxIdle;

   public PutCommandHandler(CliCommandBuffer buffer) {
      super(CacheCommand.PUT, buffer);
      putIfAbsent = new ArgumentWithoutValue(this, -1, "--ifabsent");
      new ArgumentWithValue(this, null, 1, "--value");
      expires = new ArgumentWithValue(this, new SimpleTabCompleter(new String[] { "expires" }), 2, "--expires");
      new ArgumentWithValue(this, null, 3, "--expiration");
      maxIdle = new ArgumentWithValue(this, new SimpleTabCompleter(new String[] { "maxidle" }), 4, "--max-idle");
      new ArgumentWithValue(this, null, 5, "--max-idle-time");
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
         if (!putIfAbsent.isPresent(parsedCommandLine) && size == 0) {
            argumentList.add(putIfAbsent);
         }
         if (addIfMissing(parsedCommandLine, "expires", 2)) {
            argumentList.add(expires);
         }
         if (addIfMissing(parsedCommandLine, "maxidle", 4)) {
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
         return new PutCommandHandler(CliCommandBuffer.INSTANCE);
      }

      @Override
      public boolean isTabComplete() {
         return true;
      }

      @Override
      public String[] getNames() {
         return new String[] { CacheCommand.PUT.getName() };
      }

   }
}
