package org.infinispan.test.fwk;

import org.testng.annotations.Test;

@Test(groups = "functional", testName = "test.fwk.S3TestNameVerifier")
public class S3TestNameVerifier extends TestNameVerifier {
   public S3TestNameVerifier() {
      moduleName = "cachestore/s3";
   }
}
