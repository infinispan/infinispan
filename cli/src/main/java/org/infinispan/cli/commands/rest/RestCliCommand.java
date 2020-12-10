package org.infinispan.cli.commands.rest;

import java.util.concurrent.CompletionStage;

import org.aesh.command.CommandException;
import org.aesh.command.CommandResult;
import org.aesh.command.shell.Shell;
import org.aesh.readline.terminal.formatting.Color;
import org.aesh.readline.terminal.formatting.TerminalColor;
import org.aesh.readline.terminal.formatting.TerminalString;
import org.infinispan.cli.Context;
import org.infinispan.cli.commands.CliCommand;
import org.infinispan.cli.connection.Connection;
import org.infinispan.cli.impl.ContextAwareCommandInvocation;
import org.infinispan.cli.resources.Resource;
import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.RestResponse;
import org.infinispan.commons.util.Util;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 12.0
 **/
abstract class RestCliCommand extends CliCommand {

   protected abstract CompletionStage<RestResponse> exec(ContextAwareCommandInvocation invocation, RestClient client, Resource resource);

   @Override
   protected final CommandResult exec(ContextAwareCommandInvocation invocation) throws CommandException {
      Shell shell = invocation.getShell();
      Context context = invocation.getContext();
      try {
         String response = context.getConnection().execute((c, r) -> exec(invocation, c, r), getResponseMode());
         if (response != null && !response.isEmpty()) {
            shell.writeln(response);
         }
         invocation.getContext().refreshPrompt();
         return CommandResult.SUCCESS;
      } catch (Exception e) {
         TerminalString error = new TerminalString(Util.getRootCause(e).getLocalizedMessage(), new TerminalColor(Color.RED, Color.DEFAULT, Color.Intensity.BRIGHT));
         shell.writeln(error.toString());
         invocation.getContext().refreshPrompt();
         return CommandResult.FAILURE;
      }
   }

   public Connection.ResponseMode getResponseMode() {
      return Connection.ResponseMode.BODY;
   }
}
