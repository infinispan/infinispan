package org.infinispan.commons.test;

import static org.infinispan.commons.test.RunningTestsRegistry.registerThreadWithTest;
import static org.infinispan.commons.test.RunningTestsRegistry.unregisterThreadWithTest;

import org.junit.runner.Description;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;
import org.junit.runner.notification.RunListener;
import org.junit.runner.notification.RunNotifier;
import org.junit.runners.BlockJUnit4ClassRunner;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.InitializationError;

/**
 * Logs JUnit test progress.
 *
 * <p>To enable when running a test in the IDE, annotate the test class with
 * {@code @RunWith(JUnitTestListener.Runner.class)}.</p>
 *
 * <p>To enable in Maven, set the {@code listener} property in the surefire/failsafe plugin.</p>
 *
 * @author Dan Berindei
 * @since 9.0
 */
public class JUnitTestListener extends RunListener {
   /**
    * Use this runner to add the listener to your test
    */
   public static class Runner extends BlockJUnit4ClassRunner {
      public Runner(Class<?> klass) throws InitializationError {
         super(klass);
      }

      @Override
      protected void runChild(final FrameworkMethod method, RunNotifier notifier) {
         notifier.addListener(new JUnitTestListener());

         super.runChild(method, notifier);
      }
   }

   private ThreadLocal<Boolean> currentTestIsSuccessful = new ThreadLocal<>();

   private final TestSuiteProgress progressLogger;
   private String currentTestRunName;

   public JUnitTestListener() {
      progressLogger = new TestSuiteProgress();
   }

   @Override
   public void testStarted(Description description) throws Exception {
      String testName = testName(description);
      String simpleName = description.getTestClass().getSimpleName();
      progressLogger.testStarted(testName);
      registerThreadWithTest(testName, simpleName);
      currentTestIsSuccessful.set(true);
   }

   @Override
   public void testFinished(Description description) throws Exception {
      unregisterThreadWithTest();
      if (currentTestIsSuccessful.get()) {
         progressLogger.testSucceeded(testName(description));
      }
   }

   @Override
   public void testFailure(Failure failure) {
      currentTestIsSuccessful.set(false);
      progressLogger.testFailed(testName(failure.getDescription()), failure.getException());
   }

   @Override
   public void testIgnored(Description description) {
      currentTestIsSuccessful.set(false);
      progressLogger.testIgnored(testName(description));
   }

   @Override
   public void testAssumptionFailure(Failure failure) {
      currentTestIsSuccessful.set(false);
      progressLogger.testAssumptionFailed(testName(failure.getDescription()), failure.getException());
   }

   private String testName(Description description) {
      String className = description.isSuite() ? "suite" : description.getTestClass().getSimpleName();
      return className + "." + description.getMethodName();
   }

   @Override
   public void testRunStarted(Description description) {
      TestSuiteProgress.printTestJDKInformation();
      ThreadLeakChecker.saveInitialThreads();
      currentTestRunName = description.getDisplayName();
   }

   @Override
   public void testRunFinished(Result result) {
      try {
         // We don't use @RunWith(Suite.class) so we only have a single suite
         ThreadLeakChecker.checkForLeaks(currentTestRunName);
      } catch (Throwable e) {
         progressLogger.configurationFailed("[ERROR]", e);
         throw e;
      }
   }
}
