package org.infinispan.cli.commands.server;

import java.util.Arrays;
import java.util.List;

import org.kohsuke.MetaInfServices;

@MetaInfServices(org.infinispan.cli.commands.Command.class)
public class Replace extends AbstractServerCommand {
   private final static List<String> OPTIONS = Arrays.asList("--codec=");

   @Override
   public String getName() {
      return "replace";
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
