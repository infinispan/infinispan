package org.infinispan.cli.commands;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;
import java.util.concurrent.Callable;

import org.aesh.command.Command;
import org.aesh.command.CommandDefinition;
import org.aesh.command.CommandException;
import org.aesh.command.CommandResult;
import org.aesh.command.impl.completer.FileOptionCompleter;
import org.aesh.command.option.Arguments;
import org.aesh.command.option.Option;
import org.aesh.command.shell.Shell;
import org.aesh.io.Resource;
import org.infinispan.cli.impl.ContextAwareCommandInvocation;
import org.infinispan.cli.impl.ExitCodeResultHandler;
import org.infinispan.cli.logging.Messages;
import org.infinispan.commons.util.StringPropertyReplacer;
import org.kohsuke.MetaInfServices;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
@MetaInfServices(Command.class)
@CommandDefinition(name = "run", description = "Reads and executes commands from one or more files", resultHandler = ExitCodeResultHandler.class)
public class Run extends CliCommand {

   @Arguments(required = true, completer = FileOptionCompleter.class)
   private List<Resource> arguments;

   @Option(shortName = 'h', hasValue = false, overrideRequired = true)
   protected boolean help;

   @Override
   public boolean isHelp() {
      return help;
   }

   @Override
   public CommandResult exec(ContextAwareCommandInvocation invocation) throws CommandException {
      if (arguments != null && arguments.size() > 0) {
         for (Resource resource : arguments) {
            boolean stdin = "-".equals(resource.getName());
            if (stdin) {
               Shell shell = invocation.getShell();
               processInput("<STDIN>", shell::readLine, invocation);
            } else {
               try (BufferedReader br = new BufferedReader(new InputStreamReader(resource.read()))) {
                  processInput(resource.getAbsolutePath(), br::readLine, invocation);
               } catch (IOException e) {
                  throw Messages.MSG.batchError(resource.getAbsolutePath(), 0, "", e);
               }
            }
         }
      }
      return CommandResult.SUCCESS;
   }

   private void processInput(String source, Callable<String> lineSupplier, ContextAwareCommandInvocation invocation) throws CommandException {
      int lineCount = 0;
      String line = null;
      try {
         for (line = lineSupplier.call(); line != null; line = lineSupplier.call()) {
            lineCount++;
            if (!line.startsWith("#")) {
               invocation.executeCommand("batch " + StringPropertyReplacer.replaceProperties(line));
            }
         }
      } catch (Throwable e) {
         throw Messages.MSG.batchError(source, lineCount, line, e);
      }
   }
}
