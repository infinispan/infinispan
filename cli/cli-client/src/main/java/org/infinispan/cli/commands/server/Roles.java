package org.infinispan.cli.commands.server;

public class Roles extends AbstractServerCommand {

   @Override
   public String getName() {
      return "roles";
   }

   @Override
   public int nesting() {
      return 0;
   }


}
