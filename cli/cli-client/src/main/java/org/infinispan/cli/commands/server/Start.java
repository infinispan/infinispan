package org.infinispan.cli.commands.server;

import org.kohsuke.MetaInfServices;

@MetaInfServices(org.infinispan.cli.commands.Command.class)
public class Start extends AbstractServerCommand {

   @Override
   public String getName() {
      return "start";
   }

   @Override
   public int nesting() {
      return 1;
   }
}
