package org.infinispan.cli.commands;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.aesh.command.CommandDefinition;
import org.aesh.command.CommandException;
import org.aesh.command.CommandResult;
import org.aesh.command.option.Option;
import org.aesh.command.parser.CommandLineParserException;
import org.aesh.util.completer.ShellCompletionGenerator;
import org.aesh.util.completer.ShellCompletionGenerator.ShellType;
import org.infinispan.cli.impl.ContextAwareCommandInvocation;

/**
 * Generates shell completion scripts for the CLI.
 *
 * @since 16.2
 */
@CommandDefinition(name = "completion", description = "Generates a shell completion script")
public class Completion extends CliCommand {

   private static final String NAME = "infinispan-cli";

   @Option(name = "shell", shortName = 's', description = "Target shell: BASH, ZSH, or FISH", defaultValue = "BASH")
   ShellType shell;

   @Override
   public CommandResult exec(ContextAwareCommandInvocation invocation) throws CommandException {
      try {
         invocation.println(ShellCompletionGenerator.generate(shell, CLI.class, NAME));
         return CommandResult.SUCCESS;
      } catch (CommandLineParserException e) {
         throw new CommandException("Failed to generate completion script: " + e.getMessage(), e);
      }
   }

   public static void main(String[] args) throws CommandLineParserException, IOException {
      ShellType shellType = ShellType.BASH;
      String outputPath = null;
      for (int i = 0; i < args.length; i++) {
         switch (args[i]) {
            case "--shell":
               shellType = ShellType.valueOf(args[++i]);
               break;
            default:
               outputPath = args[i];
         }
      }
      String script = ShellCompletionGenerator.generate(shellType, CLI.class, NAME);
      if (outputPath != null) {
         Files.writeString(Path.of(outputPath), script);
      } else {
         System.out.println(script);
      }
   }
}
