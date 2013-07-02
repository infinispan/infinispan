package org.infinispan.cli.commands.server;

public class Commit extends AbstractServerCommand {

   @Override
   public String getName() {
      return "commit";
   }

   @Override
   public int nesting() {
      return -1;
   }

}
