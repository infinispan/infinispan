package org.infinispan.cli.commands.server;

import org.kohsuke.MetaInfServices;

@MetaInfServices(org.infinispan.cli.commands.Command.class)
public class Deny extends AbstractServerCommand {

   @Override
   public String getName() {
      return "deny";
   }

   @Override
   public int nesting() {
      return 0;
   }


}
