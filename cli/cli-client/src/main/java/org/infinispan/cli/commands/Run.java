package org.infinispan.cli.commands;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.List;

import org.aesh.command.Command;
import org.aesh.command.CommandDefinition;
import org.aesh.command.CommandResult;
import org.aesh.command.impl.completer.FileOptionCompleter;
import org.aesh.command.option.Arguments;
import org.aesh.command.option.Option;
import org.aesh.io.Resource;
import org.infinispan.cli.impl.ContextAwareCommandInvocation;
import org.infinispan.commons.util.StringPropertyReplacer;
import org.kohsuke.MetaInfServices;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
@MetaInfServices(Command.class)
@CommandDefinition(name = Run.CMD, description = "Reads and executes commands from one or more files")
public class Run extends CliCommand {
   public static final String CMD = "run";

   @Arguments(required = true, completer = FileOptionCompleter.class)
   private List<Resource> arguments;

   @Option(shortName = 'h', hasValue = false, overrideRequired = true)
   protected boolean help;

   @Override
   public boolean isHelp() {
      return help;
   }

   @Override
   public CommandResult exec(ContextAwareCommandInvocation invocation) {

      if (arguments != null && arguments.size() > 0) {
         for (Resource resource : arguments) {
            try (BufferedReader br = new BufferedReader("-".equals(resource.getName()) ? new InputStreamReader(System.in) : new InputStreamReader(resource.read()))) {
               for (String line = br.readLine(); line != null; line = br.readLine()) {
                  if (!line.startsWith("#")) {
                     invocation.executeCommand(Batch.CMD + " " + StringPropertyReplacer.replaceProperties(line));
                  }
               }
            } catch (Exception e) {
               throw new RuntimeException(e);
            }
         }
      }
      return CommandResult.SUCCESS;
   }
}
