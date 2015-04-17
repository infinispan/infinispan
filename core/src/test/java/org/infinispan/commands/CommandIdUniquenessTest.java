package org.infinispan.commands;

import org.infinispan.commons.util.ClassFinder;
import org.infinispan.test.AbstractInfinispanTest;
import org.testng.annotations.Test;

import java.lang.reflect.Constructor;
import java.lang.reflect.Modifier;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

import static org.testng.AssertJUnit.assertNotNull;

@Test(groups = "unit", testName = "commands.CommandIdUniquenessTest")
public class CommandIdUniquenessTest extends AbstractInfinispanTest {
   public void testCommandIdUniqueness() throws Exception {
      List<Class<?>> commands = ClassFinder.isAssignableFrom(ReplicableCommand.class);
      SortedMap<Byte, String> cmdIds = new TreeMap<Byte, String>();

      for (Class<?> c : commands) {
         if (!c.isInterface() && !Modifier.isAbstract(c.getModifiers()) && !LocalCommand.class.isAssignableFrom(c)) {
            log.infof("Testing %s", c.getSimpleName());
            Constructor<?>[] declaredCtors = c.getDeclaredConstructors();
            Constructor<?> constructor = null;
            for (Constructor<?> declaredCtor : declaredCtors) {
               if (declaredCtor.getParameterTypes().length == 0) {
                  constructor = declaredCtor;
                  constructor.setAccessible(true);
                  break;
               }
            }

            assertNotNull("Empty constructor not found for " + c.getSimpleName(), constructor);
            ReplicableCommand cmd = (ReplicableCommand) constructor.newInstance();
            byte b = cmd.getCommandId();
            assert b > 0 : "Command " + c.getSimpleName() + " has a command id of " + b + " and does not implement LocalCommand!";
            assert !cmdIds.containsKey(b) : "Command ID [" + b + "] is duplicated in " + c.getSimpleName() + " and " + cmdIds.get(b);
            cmdIds.put(b, c.getSimpleName());
         }
      }
   }
}
