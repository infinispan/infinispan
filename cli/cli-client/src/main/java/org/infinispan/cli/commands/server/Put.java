package org.infinispan.cli.commands.server;

import java.util.Arrays;
import java.util.List;

public class Put extends AbstractServerCommand {
   private final static List<String> OPTIONS = Arrays.asList("--codec=", "--ifabsent");

   @Override
   public String getName() {
      return "put";
   }

   @Override
   public List<String> getOptions() {
      return OPTIONS;
   }

   @Override
   public int nesting() {
      return 0;
   }

}
