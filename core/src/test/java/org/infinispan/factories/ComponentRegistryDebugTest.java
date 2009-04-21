package org.infinispan.factories;

import org.testng.annotations.Test;

@Test(groups = "unit, functional", testName = "factories.ComponentRegistryDebugTest")
public class ComponentRegistryDebugTest {
   public void testDebugStatus() {
      assert !AbstractComponentRegistry.DEBUG_DEPENDENCIES : "Please set DEBUG_DEPENDENCIES to false!!";
   }
}
