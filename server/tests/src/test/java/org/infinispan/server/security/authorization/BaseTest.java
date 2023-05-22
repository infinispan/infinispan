package org.infinispan.server.security.authorization;

import org.junit.AssumptionViolatedException;
import org.junit.Rule;
import org.junit.rules.TestRule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

public class BaseTest {

   public static final String BANK_PROTO = "bank.proto";
   public static final String UNAUTHORIZED_EXCEPTION = "(?s).*ISPN000287.*";

   protected AbstractAuthorization suite = null;

   @Rule
   public TestRule initializeSuiteRule = new TestWatcher() {
      @Override
      public Statement apply(final Statement base, final Description description) {
         return new Statement() {
            @Override
            public void evaluate() throws Throwable {
               BaseTest.this.suite = AuthorizationSuiteRunner.suiteInstance();
               if (suite == null) throw new AssumptionViolatedException("No suite instance found");
               suite.init();

               Statement statement = suite.getServerTest().apply(base, description);
               statement.evaluate();
            }
         };
      }
   };
}
