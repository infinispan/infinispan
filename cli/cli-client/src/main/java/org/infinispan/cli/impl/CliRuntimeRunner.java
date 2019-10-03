package org.infinispan.cli.impl;

import java.io.IOException;

import org.aesh.command.CommandException;
import org.aesh.command.CommandNotFoundException;
import org.aesh.command.CommandRuntime;
import org.aesh.command.parser.CommandLineParserException;
import org.aesh.command.validator.CommandValidatorException;
import org.aesh.command.validator.OptionValidatorException;

public class CliRuntimeRunner {
   private final String comandName;
   private final CommandRuntime runtime;
   private String[] args;

   private CliRuntimeRunner(String commandName, CommandRuntime runtime) {
      this.comandName = commandName;
      this.runtime = runtime;
   }

   public static CliRuntimeRunner builder(String comandName, CommandRuntime runtime) {
      return new CliRuntimeRunner(comandName, runtime);
   }

   public CliRuntimeRunner args(String[] args) {
      this.args = args;
      return this;
   }

   public void execute() {
      StringBuilder sb = new StringBuilder(comandName);
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
         showHelpIfNeeded(runtime, comandName, e);
      } catch (InterruptedException | IOException e) {
         System.err.println(e.getMessage());
      }


   }

   private static void showHelpIfNeeded(CommandRuntime runtime, String commandName, Exception e) {
      if (e != null) {
         System.err.println(e.getMessage());
      }
      System.err.println(runtime.commandInfo(commandName));
   }
}
