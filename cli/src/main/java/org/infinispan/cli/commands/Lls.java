package org.infinispan.cli.commands;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.aesh.command.Command;
import org.aesh.command.CommandDefinition;
import org.aesh.command.CommandResult;
import org.aesh.command.impl.completer.FileOptionCompleter;
import org.aesh.command.option.Argument;
import org.infinispan.cli.impl.ContextAwareCommandInvocation;
import org.kohsuke.MetaInfServices;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 16.3
 **/
@MetaInfServices(Command.class)
@CommandDefinition(name = "lls", description = "Lists files in the current working directory")
public class Lls extends CliCommand {

   @Argument(description = "The path of the directory to list", completer = FileOptionCompleter.class)
   String path;

   @Override
   public CommandResult exec(ContextAwareCommandInvocation invocation) {
      Path dir = Paths.get(invocation.getContext().getCurrentWorkingDirectory().getAbsolutePath());
      if (path != null) {
         dir = dir.resolve(path);
      }
      if (!Files.isDirectory(dir)) {
         invocation.errorln("Not a directory: " + dir);
         return CommandResult.FAILURE;
      }
      try {
         Files.list(dir)
               .sorted(Path::compareTo)
               .forEach(r -> invocation.println(r.getFileName().toString() + (Files.isDirectory(r) ?  "/" : "")));
      } catch (IOException e) {
         return CommandResult.FAILURE;
      }
      return CommandResult.SUCCESS;
   }
}
