package org.infinispan.cli.commands.server;

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
