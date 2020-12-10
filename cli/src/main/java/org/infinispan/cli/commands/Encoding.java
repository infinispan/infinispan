package org.infinispan.cli.commands;

import org.aesh.command.Command;
import org.aesh.command.CommandDefinition;
import org.aesh.command.CommandResult;
import org.aesh.command.option.Argument;
import org.aesh.command.option.Option;
import org.infinispan.cli.completers.EncodingCompleter;
import org.infinispan.cli.impl.ContextAwareCommandInvocation;
import org.infinispan.commons.dataconversion.MediaType;
import org.kohsuke.MetaInfServices;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
@MetaInfServices(Command.class)
@CommandDefinition(name = "encoding", description = "Gets/sets the current encoding")
public class Encoding extends CliCommand {

   @Argument(completer = EncodingCompleter.class)
   String encoding;

   @Option(shortName = 'h', hasValue = false, overrideRequired = true)
   protected boolean help;

   @Override
   public boolean isHelp() {
      return help;
   }

   @Override
   public CommandResult exec(ContextAwareCommandInvocation invocation) {
      if (encoding != null) {
         invocation.getContext().setEncoding(MediaType.fromString(encoding));
      } else {
         invocation.println(invocation.getContext().getEncoding().toString());
      }
      return CommandResult.SUCCESS;
   }
}
