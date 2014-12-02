package org.infinispan.cli.commands.server;

import org.kohsuke.MetaInfServices;

@MetaInfServices(org.infinispan.cli.commands.Command.class)
public class Rollback extends AbstractServerCommand {

   @Override
   public String getName() {
      return "rollback";
   }

   @Override
   public int nesting() {
      return -1;
   }

}
