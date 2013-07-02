package org.infinispan.cli.impl;

import org.infinispan.cli.CommandBuffer;


public class CommandBufferImpl implements CommandBuffer {
   private int nesting = 0;
   StringBuilder commands = new StringBuilder(1024);

   @Override
   public void reset() {
      nesting = 0;
      commands.setLength(0);
   }

   @Override
   public boolean addCommand(String command, int nesting) {
      this.nesting += nesting;
      commands.append(command).append("\n");
      return this.nesting == 0;
   }

   @Override
   public String toString() {
      return commands.toString();
   }
}
