package org.infinispan.util;

import org.infinispan.commons.util.ClassFinder;
import org.testng.Assert;
import org.testng.annotations.Test;

@Test (groups = "functional", testName = "util.ClassFinderTest")
public class ClassFinderTest {

   public void testInfinispanClassesNonEmpty() throws Throwable {
      Assert.assertFalse(ClassFinder.infinispanClasses().isEmpty());
   }
}
