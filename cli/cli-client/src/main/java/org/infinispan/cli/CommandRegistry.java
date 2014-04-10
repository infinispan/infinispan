package org.infinispan.cli;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.infinispan.cli.commands.Command;
import org.infinispan.commons.util.ServiceFinder;

public class CommandRegistry {
   private Map<String, Command> commands;

   public CommandRegistry() {
      commands = new HashMap<String, Command>();
      for (Command cmd : ServiceFinder.load(Command.class)) {
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
