package org.infinispan.cli.commands.server;

public class ClearCache extends AbstractServerCommand {

   @Override
   public String getName() {
      return "clearcache";
   }

   @Override
   public int nesting() {
      return 0;
   }

}
