package org.infinispan.cli.shell;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.infinispan.cli.Context;
import org.infinispan.cli.commands.Argument;
import org.infinispan.cli.commands.Command;
import org.infinispan.cli.commands.ProcessedCommand;
import org.jboss.aesh.complete.CompleteOperation;
import org.jboss.aesh.complete.Completion;

public class Completer implements Completion {
   private final Context context;

   public Completer(Context context) {
      this.context = context;
   }

   @Override
   public void complete(CompleteOperation op) {
      String buffer = op.getBuffer();
      if(buffer.isEmpty()) {
         // Nothing in the buffer, return all commands
         for(String name : context.getCommandRegistry().getCommandNames()) {
            Command command = context.getCommandRegistry().getCommand(name);
            if(command.isAvailable(context)) {
               op.addCompletionCandidate(name);
            }
         }
      } else {
         ProcessedCommand procCmd = new ProcessedCommand(buffer, op.getCursor());
         if(!procCmd.isCommandComplete()) {
            // A possibly incomplete command in the buffer, return the commands that match
            for(String name : context.getCommandRegistry().getCommandNames()) {
               Command command = context.getCommandRegistry().getCommand(name);
               if(command.isAvailable(context) && name.startsWith(procCmd.getCommand())) {
                  op.addCompletionCandidate(name);
               }
            }
         } else {
            Command command = context.getCommandRegistry().getCommand(procCmd.getCommand());
            if(command.isAvailable(context)) {
               op.setOffset(op.getCursor());
               for(Argument arg : procCmd.getArguments()) {
                  if(arg.getOffset()<op.getCursor()) {
                     op.setOffset(arg.getOffset());
                  } else {
                     break;
                  }
               }
               List<String> candidates = new ArrayList<>();
               addPrefixMatches(procCmd.getCurrentArgument(), command.getOptions(), candidates);
               command.complete(context, procCmd, candidates);
               op.addCompletionCandidates(candidates);
            }
         }
      }
   }

   public static void addPrefixMatches(Argument argument, Collection<String> all, List<String> candidates) {
      if(argument==null) {
         candidates.addAll(all);
      } else {
         String prefix = argument.getValue();
         for(String s : all) {
            if(s.startsWith(prefix)) {
               candidates.add(s);
            }
         }
      }
   }
}
