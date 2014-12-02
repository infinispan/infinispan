package org.infinispan.cli.commands.server;

import java.util.Arrays;
import java.util.List;

import org.infinispan.cli.Context;
import org.infinispan.cli.commands.ProcessedCommand;
import org.infinispan.cli.shell.Completer;
import org.kohsuke.MetaInfServices;

@MetaInfServices(org.infinispan.cli.commands.Command.class)
public class Site extends AbstractServerCommand {
   private static final List<String> OPTIONS = Arrays.asList("--status", "--online", "--offline");

   @Override
   public String getName() {
      return "site";
   }

   @Override
   public List<String> getOptions() {
      return OPTIONS;
   }

   @Override
   public int nesting() {
      return 0;
   }

   @Override
   public void complete(final Context context, final ProcessedCommand procCmd, final List<String> candidates) {
      Completer.addPrefixMatches(procCmd.getCurrentArgument(), context.getConnection().getAvailableCaches(), candidates);
   }
}
