package org.infinispan.server.security.authorization;

import org.junit.runner.Description;
import org.junit.runner.RunWith;
import org.junit.runner.notification.RunListener;
import org.junit.runner.notification.RunNotifier;
import org.junit.runners.Suite;
import org.junit.runners.model.InitializationError;
import org.junit.runners.model.RunnerBuilder;

public class AuthorizationSuiteRunner extends Suite {

   private static final ThreadLocal<AbstractAuthorization> suiteClass = new ThreadLocal<>();

   public AuthorizationSuiteRunner(Class<?> klass, RunnerBuilder builder) throws InitializationError {
      super(klass, builder);
   }

   public AuthorizationSuiteRunner(RunnerBuilder builder, Class<?>[] classes) throws InitializationError {
      super(builder, classes);
   }

   @Override
   public void run(RunNotifier notifier) {
      notifier.addListener(new SuiteListener());
      super.run(notifier);
   }

   private static class SuiteListener extends RunListener {

      @Override
      public void testSuiteStarted(Description description) throws Exception {
         super.testSuiteStarted(description);
         if (!description.isSuite() || suiteClass.get() != null || description.getTestClass() == null) return;
         if (AbstractAuthorization.class.isAssignableFrom(description.getTestClass())) {
            AbstractAuthorization instance = (AbstractAuthorization) description.getTestClass().getConstructors()[0].newInstance();
            suiteClass.set(instance);
         }
      }

      @Override
      public void testSuiteFinished(Description description) throws Exception {
         super.testSuiteFinished(description);
         RunWith runWith = description.getAnnotation(RunWith.class);
         if (runWith == null || !runWith.value().equals(AuthorizationSuiteRunner.class)) return;
         suiteClass.set(null);
      }
   }

   public static AbstractAuthorization suiteInstance() {
      return suiteClass.get();
   }
}
