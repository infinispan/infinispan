package org.infinispan.cli.commands.server;

import java.util.List;

import org.infinispan.cli.Context;
import org.infinispan.cli.commands.ProcessedCommand;
import org.infinispan.cli.shell.Completer;
import org.kohsuke.MetaInfServices;

@MetaInfServices(org.infinispan.cli.commands.Command.class)
public class Create extends AbstractServerCommand {

   @Override
   public String getName() {
      return "create";
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
