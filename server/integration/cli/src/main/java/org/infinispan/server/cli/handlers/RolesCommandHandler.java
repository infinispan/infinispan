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
import org.jboss.as.cli.impl.ArgumentWithValue;
import org.jboss.as.cli.operation.ParsedCommandLine;

/**
 * The {@link CacheCommand#ROLES} handler.
 *
 * @author Tristan Tarrant
 * @since 6.1
 */
public class RolesCommandHandler extends NoArgumentsCliCommandHandler {

   private ArgumentWithValue principal;

   public RolesCommandHandler(CliCommandBuffer buffer) {
      super(CacheCommand.ROLES, buffer);
      principal = new ArgumentWithValue(this, null, 0, "--principal");
   }

   @Override
   public Collection<CommandArgument> getArguments(CommandContext ctx) {
      List<CommandArgument> argumentList = new ArrayList<CommandArgument>(1);
      try {
         ParsedCommandLine parsedCommandLine = ctx.getParsedCommandLine();
         int size = parsedCommandLine.getOtherProperties().size();
         if (!principal.isPresent(parsedCommandLine) && size == 0) {
            argumentList.add(principal);
         }
      } catch (CommandFormatException e) {
         //ignored!
      }
      return argumentList;
   }

   public static class Provider implements CommandHandlerProvider {

      @Override
      public CommandHandler createCommandHandler(CommandContext ctx) {
         return new RolesCommandHandler(CliCommandBuffer.INSTANCE);
      }

      @Override
      public boolean isTabComplete() {
         return true;
      }

      @Override
      public String[] getNames() {
         return new String[] { CacheCommand.ROLES.getName() };
      }

   }
}
