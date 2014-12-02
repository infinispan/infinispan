package org.infinispan.cli.commands.client;

import java.util.List;

import org.infinispan.cli.Context;
import org.infinispan.cli.commands.AbstractCommand;
import org.infinispan.cli.commands.ProcessedCommand;
import org.infinispan.cli.shell.Completer;
import org.kohsuke.MetaInfServices;

@MetaInfServices(org.infinispan.cli.commands.Command.class)
public class Container extends AbstractCommand {

   @Override
   public String getName() {
      return "container";
   }

   @Override
   public boolean isAvailable(Context context) {
      return context.isConnected();
   }

   @Override
   public void execute(Context context, ProcessedCommand commandLine) {
      switch (commandLine.getArguments().size()) {
      case 0:
         for (String c : context.getConnection().getAvailableContainers()) {
            context.println(c);
         }
         break;
      case 1:
         context.getConnection().setActiveContainer(commandLine.getArguments().get(0).getValue());
         break;
      default:
         context.error("Too many arguments");
      }
   }

   @Override
   public void complete(Context context, ProcessedCommand procCmd, List<String> candidates) {
      Completer.addPrefixMatches(procCmd.getCurrentArgument(), context.getConnection().getAvailableContainers(), candidates);
   }

}
