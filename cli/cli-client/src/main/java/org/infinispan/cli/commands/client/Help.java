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
package org.infinispan.cli.commands.client;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.TreeSet;

import org.infinispan.cli.Context;
import org.infinispan.cli.commands.AbstractCommand;
import org.infinispan.cli.commands.Command;
import org.infinispan.cli.commands.ProcessedCommand;
import org.infinispan.cli.shell.Completer;
import org.infinispan.cli.shell.Man2Ansi;

public class Help extends AbstractCommand {

   @Override
   public String getName() {
      return "help";
   }

   @Override
   public boolean isAvailable(Context context) {
      return true;
   }

   @Override
   public void execute(Context context, ProcessedCommand commandLine) {
      switch (commandLine.getArguments().size()) {
      case 0:
         TreeSet<String> commandNames = new TreeSet<String>(context.getCommandRegistry().getCommandNames());
         for (String name : commandNames) {
            context.println(name);
         }
         break;
      case 1:
         String name = commandLine.getArguments().get(0).getValue();
         Command command = context.getCommandRegistry().getCommand(name);
         if (command == null) {
            context.println("No such command '" + name + "'");
         } else {
            InputStream is = Thread.currentThread().getContextClassLoader()
                  .getResourceAsStream("help/" + name + ".txt");
            if (is == null) {
               context.println("No help available for command '" + name + "'");
            } else {
               try {
                  Man2Ansi man2ansi = new Man2Ansi(context.getOutputAdapter().getWidth() - 8);
                  context.println(man2ansi.render(is));
                  is.close();
               } catch (IOException e) {
               }
            }
         }
         break;
      default:
         break;
      }
   }

   @Override
   public void complete(Context context, ProcessedCommand procCmd, List<String> candidates) {
      Completer.addPrefixMatches(procCmd.getCurrentArgument(), context.getCommandRegistry().getCommandNames(), candidates);
   }
}
