package org.infinispan.cli.commands;

import java.io.IOException;

import org.aesh.command.Command;
import org.aesh.command.CommandDefinition;
import org.aesh.command.CommandException;
import org.aesh.command.CommandResult;
import org.aesh.command.option.Argument;
import org.aesh.command.option.Option;
import org.infinispan.cli.activators.ConnectionActivator;
import org.infinispan.cli.completers.CdContextCompleter;
import org.infinispan.cli.connection.Connection;
import org.infinispan.cli.impl.ContextAwareCommandInvocation;
import org.infinispan.cli.resources.Resource;
import org.kohsuke.MetaInfServices;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
@MetaInfServices(Command.class)
@CommandDefinition(name = "ls", description = "Lists resources in a path", activator = ConnectionActivator.class)
public class Ls extends CliCommand {

   @Argument(description = "The path of the subsystem/item", completer = CdContextCompleter.class)
   String path;

   @Option(shortName = 'h', hasValue = false, overrideRequired = true)
   protected boolean help;

   @Override
   public boolean isHelp() {
      return help;
   }

   @Override
   public CommandResult exec(ContextAwareCommandInvocation invocation) throws CommandException {
      try {
         Connection connection = invocation.getContext().getConnection();
         connection.refreshServerInfo();
         Resource resource = connection.getActiveResource().getResource(path);
         for (String item : resource.getChildrenNames()) {
            invocation.println(item);
         }
         return CommandResult.SUCCESS;
      } catch (IOException e) {
         throw new CommandException(e);
      }
   }
}
