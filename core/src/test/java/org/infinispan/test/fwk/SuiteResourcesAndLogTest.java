package org.infinispan.test.fwk;

import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

/**
 * This class makes sure that all files are being deleted after each test run. It also logs testsuite information.
 *
 * @author Mircea.Markus@jboss.com
 * @author Galder Zamarreño
 */
@Test(groups = "functional", testName = "test.fwk.SuiteResourcesAndLogTest")
public class SuiteResourcesAndLogTest {

   private static final Log log = LogFactory.getLog(SuiteResourcesAndLogTest.class);

   @BeforeSuite
   @AfterSuite
   public void printEnvInformation() {
      log("~~~~~~~~~~~~~~~~~~~~~~~~~ ENVIRONMENT INFO ~~~~~~~~~~~~~~~~~~~~~~~~~~");
      log("jgroups.bind_addr = " + System.getProperty("jgroups.bind_addr"));
      log("java.runtime.version = " + System.getProperty("java.runtime.version"));
      log("java.runtime.name =" + System.getProperty("java.runtime.name"));
      log("java.vm.version = " + System.getProperty("java.vm.version"));
      log("java.vm.vendor = " + System.getProperty("java.vm.vendor"));
      log("os.name = " + System.getProperty("os.name"));
      log("os.version = " + System.getProperty("os.version"));
      log("sun.arch.data.model = " + System.getProperty("sun.arch.data.model"));
      log("sun.cpu.endian = " + System.getProperty("sun.cpu.endian"));
      log("protocol.stack = " + System.getProperty("protocol.stack"));
      log("infinispan.test.jgroups.protocol = " + System.getProperty("infinispan.test.jgroups.protocol"));
      log("infinispan.unsafe.allow_jdk8_chm = [Forced: requires JDK8 now]");
      String preferIpV4 = System.getProperty("java.net.preferIPv4Stack");
      log("java.net.preferIPv4Stack = " + preferIpV4);
      log("java.net.preferIPv6Stack = " + System.getProperty("java.net.preferIPv6Stack"));
      log("log4.configurationFile = " + System.getProperty("log4j.configurationFile"));
      log("MAVEN_OPTS = " + System.getProperty("MAVEN_OPTS"));
      log("~~~~~~~~~~~~~~~~~~~~~~~~~ ENVIRONMENT INFO ~~~~~~~~~~~~~~~~~~~~~~~~~~");
   }

   private void log(String s) {
      System.out.println(s);
      log.info(s);
   }
}
