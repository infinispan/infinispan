package org.infinispan.cli.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.aesh.command.AeshCommandRuntimeBuilder;
import org.aesh.command.Command;
import org.aesh.command.CommandException;
import org.aesh.command.CommandNotFoundException;
import org.aesh.command.CommandRuntime;
import org.aesh.command.impl.registry.AeshCommandRegistryBuilder;
import org.aesh.command.invocation.CommandInvocation;
import org.aesh.command.parser.CommandLineParserException;
import org.aesh.command.registry.CommandRegistryException;
import org.aesh.command.validator.CommandValidatorException;
import org.aesh.command.validator.OptionValidatorException;

public class CliRuntimeRunner {

   private Class<? extends Command> command;
   private List<Class<? extends Command>> subCommands;
   private CommandRuntime runtime;
   private String[] args;

   private CliRuntimeRunner() {
      subCommands = new ArrayList<>();
   }

   public static CliRuntimeRunner builder() {
      return new CliRuntimeRunner();
   }

   public CliRuntimeRunner command(Class<? extends Command> command) {
      this.command = command;
      return this;
   }

   public CliRuntimeRunner subCommand(Class<? extends Command> command) {
      subCommands.add(command);
      return this;
   }

   public CliRuntimeRunner commandRuntime(CommandRuntime runtime) {
      this.runtime = runtime;
      return this;
   }

   public CliRuntimeRunner args(String[] args) {
      this.args = args;
      return this;
   }

   public void execute() {
      if (command == null && runtime == null)
         throw new RuntimeException("Command needs to be added");
      try {
         String commandName = null;
         if (runtime == null) {
            if (subCommands.isEmpty()) {
               runtime = AeshCommandRuntimeBuilder.builder().
                     commandRegistry(AeshCommandRegistryBuilder.builder().command(command).create())
                     .build();
            } else {
               //make sure we get the main command name (lets just create a tmp registry to fetch it)
               commandName = AeshCommandRegistryBuilder.builder()
                     .command(command).create().getAllCommandNames().iterator().next();
               AeshCommandRegistryBuilder<CommandInvocation> registryBuilder = AeshCommandRegistryBuilder.builder();
               registryBuilder.command(command);
               registryBuilder.commands(subCommands);
               runtime = AeshCommandRuntimeBuilder.builder().commandRegistry(registryBuilder.create()).build();
            }
         }

         if (commandName == null)
            commandName = "batch";
         StringBuilder sb = new StringBuilder(commandName);
         if (args.length > 0) {
            sb.append(" ");
            if (args.length == 1) {
               sb.append(args[0]);
            } else {
               for (String arg : args) {
                  if (arg.indexOf(' ') >= 0) {
                     sb.append('"').append(arg).append("\" ");
                  } else {
                     sb.append(arg).append(' ');
                  }
               }
            }
         }

         try {
            runtime.executeCommand(sb.toString());
         } catch (CommandNotFoundException e) {
            System.err.println("Command not found: " + sb.toString());
         } catch (CommandException | CommandLineParserException | CommandValidatorException | OptionValidatorException e) {
            showHelpIfNeeded(runtime, commandName, e);
         } catch (InterruptedException | IOException e) {
            System.err.println(e.getMessage());
         }

      } catch (CommandRegistryException e) {
         throw new RuntimeException("Exception while executing command: " + e.getMessage());
      }
   }

   private static void showHelpIfNeeded(CommandRuntime runtime, String commandName, Exception e) {
      if (e != null) {
         System.err.println(e.getMessage());
      }
      System.err.println(runtime.commandInfo(commandName));
   }
}
