package org.infinispan.cli.commands.client;

import org.infinispan.cli.Context;
import org.infinispan.cli.commands.AbstractCommand;
import org.infinispan.cli.commands.ProcessedCommand;
import org.kohsuke.MetaInfServices;

@MetaInfServices(org.infinispan.cli.commands.Command.class)
public class Disconnect extends AbstractCommand {

   @Override
   public String getName() {
      return "disconnect";
   }

   @Override
   public boolean isAvailable(Context context) {
      return context.isConnected();
   }

   @Override
   public void execute(Context context, ProcessedCommand commandLine) {
      if (context.isConnected()) {
         context.disconnect();
      }
   }

}
