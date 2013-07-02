package org.infinispan.cli.commands.server;

import java.util.List;

import org.infinispan.cli.Context;
import org.infinispan.cli.commands.ProcessedCommand;
import org.infinispan.cli.shell.Completer;

public class Cache extends AbstractServerCommand {

   @Override
   public String getName() {
      return "cache";
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
