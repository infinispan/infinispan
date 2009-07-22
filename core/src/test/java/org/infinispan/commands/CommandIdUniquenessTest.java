package org.infinispan.commands;

import org.infinispan.util.ClassFinder;
import org.testng.annotations.Test;

import java.lang.reflect.Modifier;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

@Test
public class CommandIdUniquenessTest {
   public void testCommandIdUniqueness() throws Exception {
      List<Class<?>> commands = ClassFinder.isAssignableFrom(ReplicableCommand.class);
      SortedMap<Byte, String> cmdIds = new TreeMap<Byte, String>();
      Set<String> nonReplicableCommands = new HashSet<String>();

      for (Class<?> c : commands) {
         if (!c.isInterface() && !Modifier.isAbstract(c.getModifiers())) {
            System.out.println("Testing " + c.getSimpleName());
            ReplicableCommand cmd = (ReplicableCommand) c.newInstance();
            byte b = cmd.getCommandId();
            assert !cmdIds.containsKey(b) : "Command ID [" + b + "] is duplicated in " + c.getSimpleName() + " and " + cmdIds.get(b);
            if (b <= 0)
               nonReplicableCommands.add(c.getSimpleName());
            else
               cmdIds.put(b, c.getSimpleName());
         }
      }

      // check for gaps.  First ID should be 1.
      int i = 0;
      for (Map.Entry<Byte, String> e : cmdIds.entrySet()) {
         i++;
         assert e.getKey() == i : "Expected ID " + i + " for command " + e.getValue() + " but was " + e.getKey();
      }

      System.out.println("Non-replicable commands: " + nonReplicableCommands);
      System.out.println("Next available ID is " + (i + 1));
   }
}
