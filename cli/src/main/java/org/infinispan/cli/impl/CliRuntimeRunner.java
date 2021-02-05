package org.infinispan.cli.impl;

import java.io.IOException;

import org.aesh.command.CommandException;
import org.aesh.command.CommandNotFoundException;
import org.aesh.command.CommandRuntime;
import org.aesh.command.parser.CommandLineParserException;
import org.aesh.command.validator.CommandValidatorException;
import org.aesh.command.validator.OptionValidatorException;

public class CliRuntimeRunner {
   private final String commandName;
   private final CommandRuntime runtime;
   private String[] args;

   public CliRuntimeRunner(String commandName, CommandRuntime runtime) {
      this.commandName = commandName;
      this.runtime = runtime;
   }

   public CliRuntimeRunner args(String[] args) {
      this.args = args;
      return this;
   }

   public int execute() {
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
         return ExitCodeResultHandler.exitCode;
      } catch (CommandNotFoundException e) {
         System.err.println("Command not found: " + sb);
         return 1;
      } catch (CommandException | CommandLineParserException | CommandValidatorException | OptionValidatorException e) {
         showHelpIfNeeded(runtime, commandName, e);
         return 1;
      } catch (InterruptedException | IOException e) {
         System.err.println(e.getMessage());
         return 1;
      }
   }

   private static void showHelpIfNeeded(CommandRuntime runtime, String commandName, Exception e) {
      if (e != null) {
         System.err.println(e.getMessage());
      }
      System.err.println(runtime.commandInfo(commandName));
   }
}
