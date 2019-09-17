package org.infinispan.server.test;

import org.infinispan.test.fwk.TestResourceTracker;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class TestThreadTrackerRule implements TestRule {
   @Override
   public Statement apply(Statement base, Description description) {
      return new Statement() {
         @Override
         public void evaluate() throws Throwable {
            String methodName = description.getTestClass().getSimpleName() + "." + description.getMethodName();
            TestResourceTracker.testStarted(methodName);
            try {
               methodName = description.getTestClass().getSimpleName() + "." + description.getMethodName();
               base.evaluate();
            } finally {
               TestResourceTracker.testFinished(methodName);
            }
         }
      };
   }
}
