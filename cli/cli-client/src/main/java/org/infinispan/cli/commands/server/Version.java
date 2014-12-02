package org.infinispan.cli.commands.server;

import org.infinispan.cli.Context;
import org.infinispan.cli.commands.ProcessedCommand;
import org.kohsuke.MetaInfServices;

@MetaInfServices(org.infinispan.cli.commands.Command.class)
public class Version extends AbstractServerCommand {

   @Override
   public String getName() {
      return "version";
   }

   @Override
   public int nesting() {
      return 0;
   }

   @Override
   public boolean isAvailable(Context context) {
      return true;
   }

   @Override
   public void execute(Context context, ProcessedCommand commandLine) {
      context.println("Client Version " + Version.class.getPackage().getImplementationVersion());
      if (context.isConnected()) {
         super.execute(context, commandLine);
      }
   }
}
