package org.infinispan.cli.commands.server;

import org.kohsuke.MetaInfServices;

@MetaInfServices(org.infinispan.cli.commands.Command.class)
public class Abort extends AbstractServerCommand {

   @Override
   public String getName() {
      return "abort";
   }

   @Override
   public int nesting() {
      return -1;
   }

}
