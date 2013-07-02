package org.infinispan.config;

import org.testng.annotations.Test;

/**
 * Tests the correctness of the supplied configuration files.
 *
 * @author Mircea.Markus@jboss.com
 */
@Test(groups = "functional", testName = "config.SampleConfigFilesCorrectness52Test")
public class SampleConfigFilesCorrectness52Test extends SampleConfigFilesCorrectnessTest {

   @Override
   public String getConfigFolder() {
      return "config-samples-5.2";
   }
}
