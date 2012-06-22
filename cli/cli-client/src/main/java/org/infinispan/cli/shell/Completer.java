/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License, v. 2.1.
 * This program is distributed in the hope that it will be useful, but WITHOUT A
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License,
 * v.2.1 along with this distribution; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA  02110-1301, USA.
 */
package org.infinispan.cli.shell;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.infinispan.cli.Context;
import org.infinispan.cli.commands.Argument;
import org.infinispan.cli.commands.Command;
import org.infinispan.cli.commands.ProcessedCommand;
import org.jboss.jreadline.complete.CompleteOperation;
import org.jboss.jreadline.complete.Completion;

public class Completer implements Completion {
   private final Context context;

   public Completer(Context context) {
      this.context = context;
   }

   @Override
   public void complete(CompleteOperation op) {
      String buffer = op.getBuffer();
      List<String> candidates = op.getCompletionCandidates();
      if(buffer.isEmpty()) {
         // Nothing in the buffer, return all commands
         for(String name : context.getCommandRegistry().getCommandNames()) {
            Command command = context.getCommandRegistry().getCommand(name);
            if(command.isAvailable(context)) {
               candidates.add(name);
            }
         }
      } else {
         ProcessedCommand procCmd = new ProcessedCommand(buffer, op.getCursor());
         if(!procCmd.isCommandComplete()) {
            // A possibly incomplete command in the buffer, return the commands that match
            for(String name : context.getCommandRegistry().getCommandNames()) {
               Command command = context.getCommandRegistry().getCommand(name);
               if(command.isAvailable(context) && name.startsWith(procCmd.getCommand())) {
                  candidates.add(name);
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
               addPrefixMatches(procCmd.getCurrentArgument(), command.getOptions(), candidates);
               command.complete(context, procCmd, candidates);
            }
         }
      }
      Collections.sort(candidates);
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
