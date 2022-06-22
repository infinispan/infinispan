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
import org.infinispan.cli.completers.ListFormatCompleter;
import org.infinispan.cli.completers.PrettyPrintCompleter;
import org.infinispan.cli.connection.Connection;
import org.infinispan.cli.impl.ContextAwareCommandInvocation;
import org.infinispan.cli.printers.PrettyPrinter;
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

   @Option(shortName = 'f', description = "Use a listing format (supported only by some resources)", defaultValue = "NAMES", completer = ListFormatCompleter.class)
   String format;

   @Option(shortName = 'p', name = "pretty-print", description = "Pretty-print the output", defaultValue = "TABLE", completer = PrettyPrintCompleter.class)
   String prettyPrint;

   @Option(shortName = 'l', hasValue = false, description = "Shortcut for -f FULL.")
   boolean l;

   @Option(name = "max-items", shortName = 'm', description = "Limit the number of results (supported only by some resources). Defaults to no limit", defaultValue = "-1")
   int maxItems;

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

         resource.printChildren(l ? Resource.ListFormat.FULL : Resource.ListFormat.valueOf(format), maxItems, PrettyPrinter.PrettyPrintMode.valueOf(prettyPrint), invocation.getShell());

         return CommandResult.SUCCESS;
      } catch (IOException e) {
         throw new CommandException(e);
      }
   }
}
