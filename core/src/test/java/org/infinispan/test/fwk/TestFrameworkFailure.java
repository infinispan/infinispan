package org.infinispan.test.fwk;

import org.testng.TestNGException;

public class TestFrameworkFailure {
   private final Throwable t;

   public TestFrameworkFailure(String format, Object... args) {
      this(new TestNGException(String.format(format, args)));
   }

   public TestFrameworkFailure(Throwable t) {
      this.t = t;
   }

   public void fail() throws Throwable {
      throw t;
   }
}
