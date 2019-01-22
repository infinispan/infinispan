package org.infinispan.spring.common;

import org.infinispan.test.fwk.TestResourceTracker;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.test.context.TestContext;
import org.springframework.test.context.support.AbstractTestExecutionListener;

/**
 * Ensures that we register with {@link TestResourceTracker} within the lifecycle of Spring Context tests. This is
 * because Spring calls createCacheManager from the superclass BeforeClass method and we need to manipulate the tracker
 * beforehand.
 */
public class InfinispanTestExecutionListener extends AbstractTestExecutionListener {

   @Override
   public void beforeTestClass(TestContext testContext) throws Exception {
      TestResourceTracker.testStarted(testContext.getTestClass().getName());
   }

   @Override
   public void afterTestClass(TestContext testContext) throws Exception {
      // The Spring listener that stops the application context will run later
      // So we stop it explicitly before running the thread leak check
      ((ConfigurableApplicationContext)testContext.getApplicationContext()).close();
      TestResourceTracker.testFinished(testContext.getTestClass().getName());
   }
}
