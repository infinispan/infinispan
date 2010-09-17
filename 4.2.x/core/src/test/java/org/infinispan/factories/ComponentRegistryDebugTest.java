package org.infinispan.factories;

import org.infinispan.test.AbstractInfinispanTest;
import org.testng.annotations.Test;

@Test(groups = "unit, functional", testName = "factories.ComponentRegistryDebugTest")
public class ComponentRegistryDebugTest extends AbstractInfinispanTest {
   public void testDebugStatus() {
      assert !AbstractComponentRegistry.DEBUG_DEPENDENCIES : "Please set DEBUG_DEPENDENCIES to false!!";
   }
}
