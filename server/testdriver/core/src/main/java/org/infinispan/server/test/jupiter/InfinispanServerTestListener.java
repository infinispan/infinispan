package org.infinispan.server.test.jupiter;

import org.infinispan.server.test.core.ContainerInfinispanServerDriver;
import org.junit.platform.launcher.TestExecutionListener;
import org.junit.platform.launcher.TestPlan;

public class InfinispanServerTestListener implements TestExecutionListener {
   @Override
   public void testPlanExecutionFinished(TestPlan testPlan) {
      ContainerInfinispanServerDriver.cleanup();
   }
}
