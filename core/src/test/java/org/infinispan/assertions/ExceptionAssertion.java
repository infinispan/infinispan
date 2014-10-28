package org.infinispan.assertions;

import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertTrue;

/**
 * Custom assertion for testing exceptions.
 *
 * @author Sebastian Laskawiec
 */
public class ExceptionAssertion {

   private Exception actual;

   private ExceptionAssertion(Exception actual) {
      this.actual = actual;
   }

   public static ExceptionAssertion assertThat(Exception actual) {
      return new ExceptionAssertion(actual);
   }

   public ExceptionAssertion IsNotNull() {
      assertNotNull(actual);
      return this;
   }

   public ExceptionAssertion isTypeOf(Class<? extends Exception> exceptionClass) {
      assertTrue(actual.getClass().isAssignableFrom(exceptionClass));
      return this;
   }

   public ExceptionAssertion hasCauseTypeOf(Class<?> cause) {
      assertTrue(actual.getCause().getClass().isAssignableFrom(cause));
      return this;
   }
}
