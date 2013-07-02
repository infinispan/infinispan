package org.infinispan.cli.commands.server;

public class Abort extends AbstractServerCommand {

   @Override
   public String getName() {
      return "abort";
   }

   @Override
   public int nesting() {
      return -1;
   }

}
