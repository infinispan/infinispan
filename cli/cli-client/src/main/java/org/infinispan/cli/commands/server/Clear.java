package org.infinispan.cli.commands.server;

public class Clear extends AbstractServerCommand {

   @Override
   public String getName() {
      return "clear";
   }

   @Override
   public int nesting() {
      return 0;
   }

}
