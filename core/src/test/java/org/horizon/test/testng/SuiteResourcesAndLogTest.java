package org.horizon.test.testng;

import org.horizon.logging.Log;
import org.horizon.logging.LogFactory;
import org.horizon.test.TestingUtil;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

import java.io.File;

/**
 * This class makes sure that all files are being deleted after each test run. It also logs testsuite information.
 *
 * @author Mircea.Markus@jboss.com
 */
@Test(groups = "functional", testName = "test.testng.SuiteResourcesAndLogTest")
public class SuiteResourcesAndLogTest {
   
   private static Log log = LogFactory.getLog(SuiteResourcesAndLogTest.class);

   @BeforeSuite
   @AfterSuite
   public void removeTempDir() {
      TestingUtil.recursiveFileRemove(TestingUtil.TEST_FILES);
      log("Removing all the files from " + TestingUtil.TEST_FILES);
      File file = new File(TestingUtil.TEST_FILES);
      if (file.exists()) {
         System.err.println("!!!!!!!!!!!!! Directory '" + TestingUtil.TEST_FILES + "' should have been deleted!!!");
      } else {
         log("Successfully removed folder: '" + TestingUtil.TEST_FILES + "'");
      }
   }

   @BeforeSuite
   @AfterSuite
   public void printEnvInformation() {
      log("~~~~~~~~~~~~~~~~~~~~~~~~~ ENVIRONMENT INFO ~~~~~~~~~~~~~~~~~~~~~~~~~~");
      String bindAddress = System.getProperty("bind.address");
      log("bind.address = " + bindAddress);
//      //todo for some funny reasons MVN ignores bind.address passed in. This is a hack..
//      if (bindAddress == null)
//      {
//         log("Setting bind.address to 127.0.0.1 as it is missing!!!");
//         System.setProperty("bind.address","127.0.0.1");
//      }
      log("java.runtime.version = " + System.getProperty("java.runtime.version"));
      log("java.runtime.name =" + System.getProperty("java.runtime.name"));
      log("java.vm.version = " + System.getProperty("java.vm.version"));
      log("java.vm.vendor = " + System.getProperty("java.vm.vendor"));
      log("os.name = " + System.getProperty("os.name"));
      log("os.version = " + System.getProperty("os.version"));
      log("sun.arch.data.model = " + System.getProperty("sun.arch.data.model"));
      log("sun.cpu.endian = " + System.getProperty("sun.cpu.endian"));
      log("jgroups.stack = " + System.getProperty("jgroups.stack"));
      log("~~~~~~~~~~~~~~~~~~~~~~~~~ ENVIRONMENT INFO ~~~~~~~~~~~~~~~~~~~~~~~~~~");
   }

   private void log(String s) {
      System.out.println(s);
      log.info(s);
   }
}
