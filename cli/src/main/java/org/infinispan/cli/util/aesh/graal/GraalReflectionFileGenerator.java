package org.infinispan.cli.util.aesh.graal;

import static org.aesh.terminal.utils.Config.getLineSeparator;

import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.aesh.AeshRuntimeRunner;
import org.aesh.command.Command;
import org.aesh.command.impl.internal.ProcessedCommand;
import org.aesh.command.impl.internal.ProcessedOption;
import org.aesh.command.impl.parser.AeshOptionParser;
import org.aesh.command.impl.parser.CommandLineParser;
import org.aesh.command.invocation.CommandInvocation;

public class GraalReflectionFileGenerator {

   Set<String> klasses = new HashSet<>();

   public GraalReflectionFileGenerator() {
      klasses.add(AeshOptionParser.class.getName());
   }

   public void generateReflection(CommandLineParser<CommandInvocation> parser, Writer w) throws IOException {
      w.append('[').append(getLineSeparator());
      processCommand(parser, w);
      appendOptions(w);
      w.append(getLineSeparator()).append("]");
   }

   private void processCommand(CommandLineParser<CommandInvocation> parser, Writer w) throws IOException {
      parseCommand(parser.getProcessedCommand(), w);
      if (parser.isGroupCommand()) {
         for (CommandLineParser<CommandInvocation> child : parser.getAllChildParsers()) {
            w.append("  },").append(getLineSeparator());
            processCommand(child, w);
         }
      }
   }

   private void parseCommand(ProcessedCommand<Command<CommandInvocation>, CommandInvocation> command, Writer w) throws IOException {
      w.append("  {").append(getLineSeparator());
      appendCommand(command, w);
   }

   private void appendOptions(Writer w) throws IOException {
      for (String klass : klasses) {
         w.append("  },")
               .append(getLineSeparator())
               .append("  {")
               .append(getLineSeparator())
               .append("    \"name\" : \"")
               .append(klass).append("\", ")
               .append(getLineSeparator());
         appendDefaults(w);
      }
      w.append(getLineSeparator()).append("  }");
   }

   private void appendDefaults(Writer w) throws IOException {
      w.append("    \"allDeclaredConstructors\" : true,").append(getLineSeparator())
            .append("    \"allPublicConstructors\" : true,").append(getLineSeparator())
            .append("    \"allDeclaredMethods\" : true,").append(getLineSeparator())
            .append("    \"allPublicMethods\" : true");
   }

   private void appendCommand(ProcessedCommand<Command<CommandInvocation>, CommandInvocation> command, Writer w) throws IOException {
      w.append("    \"name\" : \"").append(command.getCommand().getClass().getName()).append("\",").append(getLineSeparator());
      appendDefaults(w);
      if (command.getActivator() != null) {
         klasses.add(command.getActivator().getClass().getName());
      }
      List<ProcessedOption> allArgumentsAndOptions = new ArrayList<>(command.getOptions());
      if (command.getArguments() != null) {
         allArgumentsAndOptions.add(command.getArguments());
      }
      if (command.getArgument() != null) {
         allArgumentsAndOptions.add(command.getArgument());
      }
      if (allArgumentsAndOptions.size() > 0) {
         w.append(",").append(getLineSeparator())
               .append("    \"fields\" : [").append(getLineSeparator());
         boolean comma = false;
         for (ProcessedOption option : allArgumentsAndOptions) {
            if (comma)
               w.append(",").append(getLineSeparator());
            else
               comma = true;
            w.append("      { \"name\" : \"").append(option.getFieldName()).append("\" }");
            if (option.completer() != null) {
               klasses.add(option.completer().getClass().getName());
            }
            if (option.activator() != null) {
               klasses.add(option.activator().getClass().getName());
            }
            if (option.converter() != null) {
               klasses.add(option.converter().getClass().getName());
            }
         }
         w.append(getLineSeparator());
         w.append("    ]").append(getLineSeparator());
      }
   }

   public static void main(String[] args) {
      AeshRuntimeRunner.builder().command(GraalReflectionCommand.class).args(args).execute();
   }
}
