package org.horizon.test.fwk;

import org.testng.annotations.Test;

@Test(groups = "functional", testName = "test.fwk.JdbcTestNameVerifier")
public class JdbcTestNameVerifier extends TestNameVerifier {
   public JdbcTestNameVerifier() {
      moduleName = "cachestore/jdbc";
   }
}
