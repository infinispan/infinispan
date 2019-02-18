package org.infinispan.server.test;

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class ServerTestMethodRule implements TestRule {
   private final ServerTestRule serverTestRule;

   public ServerTestMethodRule(ServerTestRule serverTestRule) {
      assert serverTestRule != null;
      this.serverTestRule = serverTestRule;
   }

   @Override
   public Statement apply(Statement base, Description description) {
      return new Statement() {
         @Override
         public void evaluate() throws Throwable {
            before();
            try {
               ServerTestMethodConfiguration config = description.getAnnotation(ServerTestMethodConfiguration.class);
               base.evaluate();
            } finally {
               after();
            }
         }
      };
   }

   private void before() {
   }

   private void after() {
   }
}
