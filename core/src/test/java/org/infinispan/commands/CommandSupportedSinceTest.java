package org.infinispan.commands;

import static org.assertj.core.api.Fail.fail;

import java.lang.reflect.Modifier;

import org.infinispan.commons.util.ClassFinder;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.Mocks;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "commands.CommandImplementsSupportedSinceTest")
public class CommandSupportedSinceTest extends AbstractInfinispanTest {
   public void testImplementsSupportSince() throws Exception {
      for (Class<?> c : ClassFinder.isAssignableFrom(ReplicableCommand.class)) {
         if (!c.isInterface() && !Modifier.isAbstract(c.getModifiers()) && !c.getName().contains(Mocks.class.getName())) {
            log.infof("Testing %s", c.getSimpleName());
            try {
               c.getDeclaredMethod("supportedSince");
            } catch (NoSuchMethodException e) {
               fail(String.format("Command '%s' missing 'supportedSince' implementation. All commands must implement this directly and not inherit the implementation", c.getName()));
            }
         }
      }
   }
}
