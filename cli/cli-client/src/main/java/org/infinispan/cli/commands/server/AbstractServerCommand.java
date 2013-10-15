package org.infinispan.cli.commands.server;

import org.infinispan.cli.CommandBuffer;
import org.infinispan.cli.Context;
import org.infinispan.cli.commands.AbstractCommand;
import org.infinispan.cli.commands.ProcessedCommand;
import org.infinispan.cli.commands.ServerCommand;

public abstract class AbstractServerCommand extends AbstractCommand implements ServerCommand {

   @Override
   public boolean isAvailable(Context context) {
      return context.isConnected();
   }

   @Override
   public void execute(Context context, ProcessedCommand commandLine) {
      CommandBuffer commandBuffer = context.getCommandBuffer();
      if(commandBuffer.addCommand(commandLine, nesting())) {
         context.execute();
      }
   }
}
