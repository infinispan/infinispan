package org.infinispan.cli.commands.server;

public class Evict extends AbstractServerCommand {

   @Override
   public String getName() {
      return "evict";
   }

   @Override
   public int nesting() {
      return 0;
   }


}
