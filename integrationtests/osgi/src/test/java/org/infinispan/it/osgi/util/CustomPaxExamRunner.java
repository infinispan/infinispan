package org.infinispan.it.osgi.util;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Arrays;

import org.junit.experimental.categories.Category;
import org.junit.runner.Description;
import org.junit.runner.notification.RunListener;
import org.junit.runner.notification.RunNotifier;
import org.junit.runners.model.InitializationError;
import org.ops4j.pax.exam.junit.PaxExam;
import org.ops4j.pax.exam.spi.reactors.ExamReactorStrategy;

/**
 * Custom runner to work around https://issues.apache.org/jira/browse/SUREFIRE-1374
 *
 * @author Dan Berindei
 * @since 9.1
 */
public class CustomPaxExamRunner extends PaxExam {
   private static InputStream savedIn;

   public CustomPaxExamRunner(Class<?> klass) throws InitializationError {
      super(replaceSystemIn(klass));
   }

   private static Class<?> replaceSystemIn(Class<?> klass) {
      // Replace System.in so that KarafJavaRunner's can't steal master commands from surefire's CommandReader
      savedIn = System.in;

      System.setIn(new ByteArrayInputStream(new byte[0]));
      return klass;
   }

   @Override
   public void run(RunNotifier notifier) {
      notifier.addListener(new RunListener() {

         @Override
         public void testStarted(Description description) throws Exception {
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

         @Override
         public void testRunStarted(Description description) throws Exception {
            super.testRunStarted(description);
         }
      });
      super.run(notifier);
   }
}
