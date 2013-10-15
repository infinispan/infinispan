package org.infinispan.cli.commands.server;

import org.infinispan.cli.CommandBuffer;
import org.infinispan.cli.Context;
import org.infinispan.cli.commands.ProcessedCommand;
import org.infinispan.cli.impl.CommandBufferImpl;

public class Ping extends AbstractServerCommand {

   @Override
   public String getName() {
      return "ping";
   }

   @Override
   public int nesting() {
      return 0;
   }

   @Override
   public boolean isAvailable(Context context) {
      return context.isConnected();
   }

   @Override
   public void execute(Context context, ProcessedCommand commandLine) {
      CommandBuffer commandBuffer = new CommandBufferImpl();
      commandBuffer.addCommand(commandLine, nesting());
      context.getConnection().execute(context, commandBuffer);
   }
}
