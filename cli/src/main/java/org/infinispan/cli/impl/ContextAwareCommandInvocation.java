package org.infinispan.cli.impl;

import java.io.IOException;
import java.io.PrintStream;

import org.aesh.command.CommandException;
import org.aesh.command.CommandNotFoundException;
import org.aesh.command.Executor;
import org.aesh.command.invocation.CommandInvocation;
import org.aesh.command.invocation.CommandInvocationConfiguration;
import org.aesh.command.parser.CommandLineParserException;
import org.aesh.command.shell.Shell;
import org.aesh.command.validator.CommandValidatorException;
import org.aesh.command.validator.OptionValidatorException;
import org.aesh.readline.Prompt;
import org.aesh.readline.action.KeyAction;
import org.infinispan.cli.Context;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class ContextAwareCommandInvocation implements CommandInvocation<ContextAwareCommandInvocation> {
   private final CommandInvocation invocation;
   private final Context context;
   private final Shell shell;

   public ContextAwareCommandInvocation(CommandInvocation commandInvocation, Context context) {
      this.invocation = commandInvocation;
      this.context = context;
      this.shell = commandInvocation.getShell();
   }

   @Override
   public Shell getShell() {
      return shell;
   }

   @Override
   public void setPrompt(Prompt prompt) {
      invocation.setPrompt(prompt);
   }

   @Override
   public Prompt getPrompt() {
      return invocation.getPrompt();
   }

   @Override
   public String getHelpInfo(String commandName) {
      return invocation.getHelpInfo(commandName);
   }

   @Override
   public String getHelpInfo() {
      return invocation.getHelpInfo();
   }

   @Override
   public void stop() {
      invocation.stop();
   }

   @Override
   public CommandInvocationConfiguration getConfiguration() {
      return invocation.getConfiguration();
   }

   @Override
   public KeyAction input() throws InterruptedException {
      return invocation.input();
   }

   @Override
   public String inputLine() throws InterruptedException {
      return invocation.inputLine();
   }

   @Override
   public String inputLine(Prompt prompt) throws InterruptedException {
      return invocation.inputLine(prompt);
   }

   @Override
   public void executeCommand(String input) throws CommandNotFoundException, CommandLineParserException, OptionValidatorException, CommandValidatorException, CommandException, InterruptedException, IOException {
      invocation.executeCommand(input);
   }

   @Override
   public Executor<ContextAwareCommandInvocation> buildExecutor(String line) throws CommandNotFoundException, CommandLineParserException, OptionValidatorException, CommandValidatorException, IOException {
      return invocation.buildExecutor(line);
   }

   @Override
   public void print(String msg) {
      invocation.print(msg);
   }

   @Override
   public void println(String msg) {
      invocation.println(msg);
   }

   public void printf(String format, Object... args) {
      invocation.print(String.format(format, args));
   }

   @Override
   public void print(String msg, boolean paging) {
      invocation.print(msg, paging);
   }

   @Override
   public void println(String msg, boolean paging) {
      invocation.println(msg, paging);
   }

   public Context getContext() {
      return context;
   }

   public PrintStream getShellOutput() {
      return System.out;
   }

   public PrintStream getShellError() {
      return System.err;
   }

   public String getPasswordInteractively(String prompt, String confirmPrompt) throws InterruptedException {
      String password = null;
      while (password == null || password.isEmpty()) {
         password = shell.readLine(new Prompt(prompt, '*'));
      }
      if (confirmPrompt != null) {
         String confirm = null;
         while (confirm == null || !confirm.equals(password)) {
            confirm = shell.readLine(new Prompt(confirmPrompt, '*'));
         }
      }
      return password;
   }
}
