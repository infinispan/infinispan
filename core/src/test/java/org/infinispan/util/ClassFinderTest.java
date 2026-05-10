package org.infinispan.util;

import static org.junit.jupiter.api.Assertions.assertFalse;

import org.infinispan.commons.util.ClassFinder;
import org.testng.annotations.Test;

@Test (groups = "functional", testName = "util.ClassFinderTest")
public class ClassFinderTest {

   public void testInfinispanClassesNonEmpty() throws Throwable {
      assertFalse(ClassFinder.infinispanClasses().isEmpty());
   }
}
