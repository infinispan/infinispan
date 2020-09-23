package org.infinispan.cli.impl;

import java.io.IOException;
import java.io.PrintStream;
import java.util.Collections;
import java.util.List;

import org.aesh.command.CommandException;
import org.aesh.command.CommandNotFoundException;
import org.aesh.command.CommandResult;
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
import org.infinispan.cli.commands.CommandInputLine;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class ContextAwareCommandInvocation<CI extends CommandInvocation> implements CommandInvocation {
   private final CommandInvocation<CI> invocation;
   private final Context context;

   public ContextAwareCommandInvocation(CommandInvocation<CI> commandInvocation, Context context) {
      this.invocation = commandInvocation;
      this.context = context;
   }

   @Override
   public Shell getShell() {
      return invocation.getShell();
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
   public Executor<CI> buildExecutor(String line) throws CommandNotFoundException, CommandLineParserException, OptionValidatorException, CommandValidatorException, IOException {
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

   public CommandResult execute(CommandInputLine cmd) {
      return context.execute(invocation.getShell(), Collections.singletonList(cmd));
   }

   public CommandResult execute(List<CommandInputLine> cmds) {
      return context.execute(invocation.getShell(), cmds);
   }

   public PrintStream getShellOutput() {
      return new PrintStream(new ShellOutputStreamAdapter(invocation.getShell()));
   }

   public PrintStream getShellError() {
      return new PrintStream(new ShellOutputStreamAdapter(invocation.getShell()));
   }
}
