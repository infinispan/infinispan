package org.infinispan.cli.commands;

import java.util.List;

import org.aesh.command.Command;
import org.aesh.command.CommandDefinition;
import org.aesh.command.CommandResult;
import org.aesh.command.option.Arguments;
import org.aesh.command.option.Option;
import org.infinispan.cli.impl.ContextAwareCommandInvocation;
import org.kohsuke.MetaInfServices;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0z`
 **/
@MetaInfServices(Command.class)
@CommandDefinition(name = "echo", description = "Echoes messages to the output. Useful for adding information to batch runs.")
public class Echo extends CliCommand {

   @Arguments
   private List<String> arguments;

   @Option(shortName = 'h', hasValue = false, overrideRequired = true)
   protected boolean help;

   @Override
   public boolean isHelp() {
      return help;
   }

   @Override
   public CommandResult exec(ContextAwareCommandInvocation invocation) {
      if (arguments != null && !arguments.isEmpty()) {
         for (int i = 0; i < arguments.size(); i++) {
            if (i > 0)
               invocation.print(" ");
            invocation.print(arguments.get(i));
         }
         invocation.println("");
      }
      return CommandResult.SUCCESS;
   }
}
