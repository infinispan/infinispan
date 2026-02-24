package org.infinispan.server.test.core;

import org.junit.AssumptionViolatedException;

public class TestSetupUtil {

   private TestSetupUtil() { }

   /**
    * Determines whether an exception represents a violated test assumption.
    *
    * <p>
    * Test assumptions may be violated deep within the exception chain during complex server initialization. This method
    * examines the entire cause chain to detect assumption violations that would otherwise be hidden by wrapping exceptions.
    * </p>
    *
    * @param t the throwable to examine, typically caught during test setup
    * @return {@code true} if the exception chain contains an {@link AssumptionViolatedException},
    *         indicating the test should be skipped rather than failed
    */

   public static boolean isAssumptionViolated(Throwable t) {
      Throwable cause = t;
      while (!(cause instanceof AssumptionViolatedException) && cause.getCause() != null)
         cause = cause.getCause();

      return cause instanceof AssumptionViolatedException;
   }
}
