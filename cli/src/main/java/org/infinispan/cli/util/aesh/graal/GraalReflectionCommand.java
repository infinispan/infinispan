package org.infinispan.cli.util.aesh.graal;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

import org.aesh.command.Command;
import org.aesh.command.CommandDefinition;
import org.aesh.command.CommandException;
import org.aesh.command.CommandResult;
import org.aesh.command.container.CommandContainer;
import org.aesh.command.container.CommandContainerBuilder;
import org.aesh.command.impl.container.AeshCommandContainerBuilder;
import org.aesh.command.invocation.CommandInvocation;
import org.aesh.command.option.Argument;
import org.aesh.command.option.Option;
import org.aesh.command.parser.CommandLineParserException;

/**
 * @author <a href="mailto:stalep@gmail.com">Ståle Pedersen</a>
 */
@CommandDefinition(name = "graalreflection", description = "Generates a json file to help graal generate a native image")
public class GraalReflectionCommand implements Command {

   @Option(hasValue = false)
   private boolean help;

   @Argument(required = true, description = "Command class name")
   private String command;


   @Override
   public CommandResult execute(CommandInvocation commandInvocation) throws CommandException, InterruptedException {
      if (help) {
         commandInvocation.getHelpInfo("graalreflection");
      } else {
         Class<Command<CommandInvocation>> clazz = loadCommand(command);
         if (clazz != null) {
            CommandContainerBuilder<CommandInvocation> builder = new AeshCommandContainerBuilder<>();
            try {
               CommandContainer<CommandInvocation> container = builder.create(clazz);
               GraalReflectionFileGenerator graalFileGenerator = new GraalReflectionFileGenerator();
               try(BufferedWriter w = Files.newBufferedWriter(Paths.get(container.getParser().getProcessedCommand().name().toLowerCase() + "_reflection.json"), StandardOpenOption.CREATE)) {
                   graalFileGenerator.generateReflection(container.getParser(), w);
               }
               container.getParser().getProcessedCommand();

            } catch (CommandLineParserException | IOException e) {
               e.printStackTrace();
            }
         } else
            commandInvocation.println("Could not load command: " + command);
      }
      return CommandResult.SUCCESS;
   }

   @SuppressWarnings("unchecked")
   private Class<Command<CommandInvocation>> loadCommand(String commandName) {
      try {
         return (Class<Command<CommandInvocation>>) Class.forName(commandName);
      } catch (ClassNotFoundException | ClassCastException e) {
         throw new RuntimeException(e);
      }
   }
}
