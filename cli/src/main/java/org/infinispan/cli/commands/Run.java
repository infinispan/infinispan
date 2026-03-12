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
import org.infinispan.cli.completers.OnErrorCompleter;
import org.infinispan.cli.converters.OnErrorConverter;
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
   List<Resource> arguments;

   @Option(name = "on-error", description = "Action to take when a command fails", completer = OnErrorCompleter.class, defaultValue = "FAIL_FAST", converter = OnErrorConverter.class)
   OnErrorCompleter.OnError onError;

   @Option(description = "Whether to echo commands to the output", hasValue = false)
   boolean echo;

   @Option(shortName = 'h', hasValue = false, overrideRequired = true)
   protected boolean help;

   @Override
   public boolean isHelp() {
      return help;
   }

   @Override
   public CommandResult exec(ContextAwareCommandInvocation invocation) throws CommandException {
      boolean failed = false;
      if (arguments != null && !arguments.isEmpty()) {
         for (Resource resource : arguments) {
            boolean stdin = "-".equals(resource.getName());
            CommandResult result;
            if (stdin) {
               Shell shell = invocation.getShell();
               result = processInput("<STDIN>", shell::readLine, invocation);
            } else {
               try (BufferedReader br = new BufferedReader(new InputStreamReader(resource.read()))) {
                  result = processInput(resource.getAbsolutePath(), br::readLine, invocation);
               } catch (IOException e) {
                  invocation.errorln(Messages.MSG.batchError(resource.getAbsolutePath(), 0, "", e));
                  result = CommandResult.FAILURE;
               }
            }
            if (result == CommandResult.FAILURE) {
               switch (onError) {
                  case FAIL_FAST:
                     return CommandResult.FAILURE;
                  case FAIL_AT_END:
                     failed = true;
               }
            }
         }
      }
      return failed ? CommandResult.FAILURE : CommandResult.SUCCESS;
   }

   private CommandResult processInput(String source, Callable<String> lineSupplier, ContextAwareCommandInvocation invocation) {
      int lineCount = 0;
      String line = "";
      boolean failed = false;
      try {
         for (line = lineSupplier.call(); line != null; line = lineSupplier.call()) {
            lineCount++;
            if (!line.startsWith("#")) {
               if (echo) {
                  invocation.println(line);
               }
               try {
                  invocation.executeCommand("batch " + StringPropertyReplacer.replaceProperties(line));
               } catch (Exception e) {
                  invocation.errorln(Messages.MSG.batchError(source, lineCount, line, e));
                  failed = true;
               }
               if (ExitCodeResultHandler.exitCode() > 0) {
                  invocation.errorln(Messages.MSG.batchError(source, lineCount, line));
                  switch (onError) {
                     case FAIL_FAST:
                        return CommandResult.FAILURE;
                     case FAIL_AT_END:
                        failed = true;
                     case IGNORE:
                        ExitCodeResultHandler.reset();
                  }
               }
            }
         }
      } catch (Exception e) {
         invocation.errorln(Messages.MSG.batchError(source, lineCount, line, e));
         failed = true;
      }
      return failed ? CommandResult.FAILURE : CommandResult.SUCCESS;
   }
}
