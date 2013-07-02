package org.infinispan.cli.commands.server;

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
