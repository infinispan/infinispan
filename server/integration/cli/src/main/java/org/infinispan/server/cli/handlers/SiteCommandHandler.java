package org.infinispan.server.cli.handlers;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import org.infinispan.server.cli.util.CliCommandBuffer;
import org.jboss.as.cli.CommandArgument;
import org.jboss.as.cli.CommandContext;
import org.jboss.as.cli.CommandFormatException;
import org.jboss.as.cli.CommandHandler;
import org.jboss.as.cli.CommandHandlerProvider;
import org.jboss.as.cli.impl.ArgumentWithValue;
import org.jboss.as.cli.impl.ArgumentWithoutValue;
import org.jboss.as.cli.operation.ParsedCommandLine;

/**
 * The {@link CacheCommand#SITE} handler.
 *
 * @author Pedro Ruivo
 * @since 6.1
 */
public class SiteCommandHandler extends NoArgumentsCliCommandHandler {

   private final ArgumentWithoutValue status;
   private final ArgumentWithoutValue online;
   private final ArgumentWithoutValue offline;

   public SiteCommandHandler(CliCommandBuffer buffer) {
      super(CacheCommand.SITE, buffer);
      status = new ArgumentWithoutValue(this, -1, "--status");
      online = new ArgumentWithoutValue(this, -1, "--online");
      offline = new ArgumentWithoutValue(this, -1, "--offline");
      new ArgumentWithValue(this, null, 0, "--site-name");
   }

   @Override
   public Collection<CommandArgument> getArguments(CommandContext ctx) {
      ParsedCommandLine parsedCommandLine = ctx.getParsedCommandLine();
      try {
         if (!status.isPresent(parsedCommandLine) && !online.isPresent(parsedCommandLine)
               && !offline.isPresent(parsedCommandLine)) {
            return Arrays.<CommandArgument> asList(status, online, offline);
         }
      } catch (CommandFormatException e) {
         //ignored!
      }
      return Collections.emptyList();
   }

   public static class Provider implements CommandHandlerProvider {

      @Override
      public CommandHandler createCommandHandler(CommandContext ctx) {
         return new SiteCommandHandler(CliCommandBuffer.INSTANCE);
      }

      @Override
      public boolean isTabComplete() {
         return true;
      }

      @Override
      public String[] getNames() {
         return new String[] { CacheCommand.SITE.getName() };
      }

   }
}
