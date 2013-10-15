package org.infinispan.cli.impl;

import org.infinispan.cli.CommandBuffer;
import org.infinispan.cli.commands.ProcessedCommand;

import java.util.ArrayList;
import java.util.List;


public class CommandBufferImpl implements CommandBuffer {
   private int nesting = 0;
   StringBuilder commandsString = new StringBuilder(1024);
   List<ProcessedCommand> commands = new ArrayList<ProcessedCommand>(1);

   @Override
   public void reset() {
      nesting = 0;
      commandsString.setLength(0);
      commands.clear();
   }

   @Override
   public boolean addCommand(ProcessedCommand commandLine, int nesting) {
      this.nesting += nesting;
      commandsString.append(commandLine.getCommandLine()).append("\n");
      commands.add(commandLine);
      return this.nesting == 0;
   }

   @Override
   public List<ProcessedCommand> getBufferedCommands() {
      return commands;
   }

   @Override
   public String toString() {
      return commandsString.toString();
   }
}
