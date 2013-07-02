package org.infinispan.cli.commands.server;

public class End extends AbstractServerCommand {

   @Override
   public String getName() {
      return "end";
   }

   @Override
   public int nesting() {
      return -1;
   }

}
