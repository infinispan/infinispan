package org.infinispan.cli.commands.server;

public class Grant extends AbstractServerCommand {

   @Override
   public String getName() {
      return "grant";
   }

   @Override
   public int nesting() {
      return 0;
   }


}
