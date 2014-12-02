package org.infinispan.cli.commands.server;

import org.kohsuke.MetaInfServices;

@MetaInfServices(org.infinispan.cli.commands.Command.class)
public class Remove extends AbstractServerCommand {

   @Override
   public String getName() {
      return "remove";
   }

   @Override
   public int nesting() {
      return 0;
   }
}
