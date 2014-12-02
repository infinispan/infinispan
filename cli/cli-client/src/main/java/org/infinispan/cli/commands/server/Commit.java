package org.infinispan.cli.commands.server;

import org.kohsuke.MetaInfServices;

@MetaInfServices(org.infinispan.cli.commands.Command.class)
public class Commit extends AbstractServerCommand {

   @Override
   public String getName() {
      return "commit";
   }

   @Override
   public int nesting() {
      return -1;
   }

}
