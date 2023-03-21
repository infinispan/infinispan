package org.infinispan.server.test.junit4;

import org.infinispan.server.test.core.ContainerInfinispanServerDriver;
import org.junit.runner.Result;
import org.junit.runner.notification.RunListener;

public class InfinispanServerTestListener extends RunListener {
   @Override
   public void testRunFinished(Result result) {
      ContainerInfinispanServerDriver.cleanup();
   }
}
