package org.infinispan.commands;

import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.util.ClassFinder;
import org.testng.annotations.Test;

import java.lang.reflect.Modifier;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

@Test(groups = "unit", testName = "commands.CommandIdUniquenessTest")
public class CommandIdUniquenessTest extends AbstractInfinispanTest {
   public void testCommandIdUniqueness() throws Exception {
      List<Class<?>> commands = ClassFinder.isAssignableFrom(ReplicableCommand.class);
      SortedMap<Byte, String> cmdIds = new TreeMap<Byte, String>();

      for (Class<?> c : commands) {
         if (!c.isInterface() && !Modifier.isAbstract(c.getModifiers()) && !LocalCommand.class.isAssignableFrom(c)) {
            System.out.println("Testing " + c.getSimpleName());
            ReplicableCommand cmd = (ReplicableCommand) c.newInstance();
            byte b = cmd.getCommandId();
            assert b > 0 : "Command " + c.getSimpleName() + " has a command id of " + b + " and does not implement LocalCommand!";
            assert !cmdIds.containsKey(b) : "Command ID [" + b + "] is duplicated in " + c.getSimpleName() + " and " + cmdIds.get(b);
            cmdIds.put(b, c.getSimpleName());
         }
      }

      // check for gaps.  First ID should be 1.
      int i = 0;
      for (Map.Entry<Byte, String> e : cmdIds.entrySet()) {
         i++;
         assert e.getKey() == i : "Expected ID " + i + " for command " + e.getValue() + " but was " + e.getKey();
      }

      System.out.println("Next available ID is " + (i + 1));
   }
}
