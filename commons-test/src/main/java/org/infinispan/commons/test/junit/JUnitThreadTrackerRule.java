package org.infinispan.commons.test.junit;

import org.infinispan.commons.test.ThreadLeakChecker;
import org.infinispan.commons.test.TestResourceTracker;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class JUnitThreadTrackerRule implements TestRule {
   @Override
   public Statement apply(Statement base, Description description) {
      return new Statement() {
         @Override
         public void evaluate() throws Throwable {
            String testName = description.getTestClass().getName();
            if (description.getMethodName() != null) {
               throw new IllegalArgumentException(String.format("Please use TestThreadTrackerRule with @ClassRule, %s is using @Rule", testName));
            }
            TestResourceTracker.testStarted(testName);
            ThreadLeakChecker.testStarted(testName);
            try {
               base.evaluate();
            } finally {
               TestResourceTracker.testFinished(testName);
               ThreadLeakChecker.testFinished(testName);
            }
         }
      };
   }
}
