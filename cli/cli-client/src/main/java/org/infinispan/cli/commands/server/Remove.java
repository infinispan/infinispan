package org.infinispan.cli.commands.server;

public class Remove extends AbstractServerCommand {

   @Override
   public String getName() {
      return "remove";
   }

   @Override
   public int nesting() {
      return 0;
   }
}
