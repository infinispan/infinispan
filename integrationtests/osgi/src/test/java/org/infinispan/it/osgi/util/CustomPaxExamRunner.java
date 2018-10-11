package org.infinispan.it.osgi.util;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.util.Arrays;

import org.infinispan.commons.util.Util;
import org.junit.experimental.categories.Category;
import org.junit.runner.Description;
import org.junit.runner.notification.RunListener;
import org.junit.runner.notification.RunNotifier;
import org.junit.runners.model.InitializationError;
import org.ops4j.pax.exam.junit.PaxExam;
import org.ops4j.pax.exam.spi.reactors.ExamReactorStrategy;
import org.ops4j.pax.exam.spi.reactors.PerSuite;
import org.ops4j.pax.exam.spi.reactors.SingletonStagedReactor;

/**
 * Custom runner to work around https://issues.apache.org/jira/browse/SUREFIRE-1374
 *
 * @author Dan Berindei
 * @since 9.1
 */
public class CustomPaxExamRunner extends PaxExam {
   private static InputStream savedIn;
   private static Class<?> hookTriggerClass = null;

   private final Class<?> testClass;

   public CustomPaxExamRunner(Class<?> klass) throws InitializationError {
      // Replace System.in in constructor instead of a static constructor
      // The static constructor might be called before Surefire started pumping System.in
      super(replaceSystemIn(klass));
      testClass = klass;
   }

   private static Class<?> replaceSystemIn(Class<?> klass) {
      if (savedIn == null) {
         // Replace System.in so that KarafJavaRunner's can't steal master commands from surefire's CommandReader
         savedIn = System.in;

         System.setIn(new ByteArrayInputStream(Util.EMPTY_BYTE_ARRAY));
      }
      return klass;
   }

   @Override
   public void run(RunNotifier notifier) {
      // Per-suite Karaf container is not stopped when some test classes are filtered by strategy
      // Add here the shutdown hook promised in the SingletonStagedReactor javadoc
      ExamReactorStrategy strategy = testClass.getAnnotation(ExamReactorStrategy.class);
      if (strategy == null || Arrays.asList(strategy.value()).contains(PerSuite.class)) {
         addShutdownHook(testClass);
      }

      // Require matching @Category and @ExamReactorStrategy annotations on all tests
      notifier.addListener(new RunListener() {
         @Override
         public void testStarted(Description description) {
            requireMatchingAnnotations(description);
         }

         @Override
         public void testRunStarted(Description description) {
         }
      });
      super.run(notifier);
   }

   private void requireMatchingAnnotations(Description description) {
      Class<?> testClass = description.getTestClass();
      if (testClass == null)
         return;

      Category categoryAnnotation = testClass.getAnnotation(Category.class);
      if (categoryAnnotation == null)
         throw new IllegalStateException(
               "Class " + testClass.getName() + " doesn't have a @Category annotation. " +
                     "All tests in the integrationtests/osgi module must have " +
                     "matching @Category and @ExamReactorStrategy annotations.");

      ExamReactorStrategy reactorStrategyAnnotation = testClass.getAnnotation(ExamReactorStrategy.class);
      if (reactorStrategyAnnotation == null) {
         throw new IllegalStateException(
               "Class " + testClass.getName() + " doesn't have a @ExamReactorStrategy annotation. " +
                     "All tests in the integrationtests/osgi module must have " +
                     "matching @Category and @ExamReactorStrategy annotations.");
      }

      if (!Arrays.equals(categoryAnnotation.value(), reactorStrategyAnnotation.value())) {
         throw new IllegalStateException(
               "The @Category and @ExamReactorStrategy annotations in class " + testClass.getName() +
                     " do not match.");
      }
   }

   private static void addShutdownHook(Class<?> klass) {
      if (hookTriggerClass != null)
         return;

      hookTriggerClass = klass;
      Runtime.getRuntime().addShutdownHook(new Thread("CustomPaxExamRunner-ShutdownHook") {
         @Override
         public void run() {
            stopSingletonStagedReactor();
         }
      });
   }

   private static void stopSingletonStagedReactor() {
      try {
         Field instanceField = SingletonStagedReactor.class.getDeclaredField("instance");
         instanceField.setAccessible(true);
         SingletonStagedReactor instance = (SingletonStagedReactor) instanceField.get(null);

         System.out.println("Forcing shutdown of Karaf container for test " + hookTriggerClass);
         instance.afterSuite();
      } catch (Exception e) {
         System.err.println("Failed to shut down suite reactor: " + e.getMessage());
      }
   }
}
