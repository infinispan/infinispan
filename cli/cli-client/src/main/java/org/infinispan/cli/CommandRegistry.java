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
package org.infinispan.cli;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;

import org.infinispan.cli.commands.Command;

public class CommandRegistry {
   private Map<String, Command> commands;

   public CommandRegistry() {
      commands = new HashMap<String, Command>();
      for (Command cmd : ServiceLoader.load(Command.class)) {
         String name = cmd.getName();
         if (commands.containsKey(name)) {
            throw new RuntimeException("Command " + cmd.getClass().getName() + " overrides "
                  + commands.get(name).getClass().getName());
         }
         commands.put(name, cmd);
      }
   }

   public Set<String> getCommandNames() {
      return Collections.unmodifiableSet(commands.keySet());
   }

   public Command getCommand(String name) {
      return commands.get(name);
   }

}
