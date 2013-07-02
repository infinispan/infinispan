package org.infinispan.cli.commands.server;

public class Start extends AbstractServerCommand {

   @Override
   public String getName() {
      return "start";
   }

   @Override
   public int nesting() {
      return 1;
   }
}
