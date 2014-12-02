package org.infinispan.cli.commands.server;

import org.kohsuke.MetaInfServices;

@MetaInfServices(org.infinispan.cli.commands.Command.class)
public class Begin extends AbstractServerCommand {

   @Override
   public String getName() {
      return "begin";
   }

   @Override
   public int nesting() {
      return 1;
   }

}
