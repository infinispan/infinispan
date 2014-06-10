package org.infinispan.cli.commands.server;

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
