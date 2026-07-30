package org.infinispan.cli.commands;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.aesh.command.Command;
import org.aesh.command.CommandDefinition;
import org.aesh.command.CommandException;
import org.aesh.command.CommandResult;
import org.aesh.command.impl.completer.FileOptionCompleter;
import org.aesh.command.option.Argument;
import org.aesh.io.FileResource;
import org.infinispan.cli.impl.ContextAwareCommandInvocation;
import org.kohsuke.MetaInfServices;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 16.3
 **/
@MetaInfServices(Command.class)
@CommandDefinition(name = "lcd", description = "Changes the local working directory")
public class Lcd extends CliCommand {

   @Argument(description = "The new path", required = true, completer = FileOptionCompleter.class)
   String path;

   @Override
   public CommandResult exec(ContextAwareCommandInvocation invocation) throws CommandException {
      Path cwd = Paths.get(invocation.getContext().getCurrentWorkingDirectory().getAbsolutePath());
      Path newPath = Paths.get(path);
      Path newCwd = newPath.isAbsolute() ? newPath : cwd.resolve(newPath);
      if (Files.isDirectory(newCwd)) {
         invocation.getContext().setCurrentWorkingDirectory(new FileResource(newCwd.toAbsolutePath()));
         return CommandResult.SUCCESS;
      } else {
         return CommandResult.FAILURE;
      }
   }
}
