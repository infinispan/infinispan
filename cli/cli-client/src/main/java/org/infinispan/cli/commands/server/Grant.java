package org.infinispan.cli.commands.server;

import org.kohsuke.MetaInfServices;

@MetaInfServices(org.infinispan.cli.commands.Command.class)
public class Grant extends AbstractServerCommand {

   @Override
   public String getName() {
      return "grant";
   }

   @Override
   public int nesting() {
      return 0;
   }


}
