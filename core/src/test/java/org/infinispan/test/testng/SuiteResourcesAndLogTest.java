package org.infinispan.test.testng;

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
@Test(groups = "functional", testName = "test.testng.SuiteResourcesAndLogTest")
public class SuiteResourcesAndLogTest {

   private static Log log = LogFactory.getLog(SuiteResourcesAndLogTest.class);


   @BeforeSuite
   @AfterSuite
   public void printEnvInformation() {
      log("~~~~~~~~~~~~~~~~~~~~~~~~~ ENVIRONMENT INFO ~~~~~~~~~~~~~~~~~~~~~~~~~~");
      String bindAddress = System.getProperty("bind.address");
      log("bind.address = " + bindAddress);
      log("java.runtime.version = " + System.getProperty("java.runtime.version"));
      log("java.runtime.name =" + System.getProperty("java.runtime.name"));
      log("java.vm.version = " + System.getProperty("java.vm.version"));
      log("java.vm.vendor = " + System.getProperty("java.vm.vendor"));
      log("os.name = " + System.getProperty("os.name"));
      log("os.version = " + System.getProperty("os.version"));
      log("sun.arch.data.model = " + System.getProperty("sun.arch.data.model"));
      log("sun.cpu.endian = " + System.getProperty("sun.cpu.endian"));
      log("protocol.stack = " + System.getProperty("protocol.stack"));
      log("infinispan.marshaller.class = " + System.getProperty("infinispan.marshaller.class"));
      String preferIpV4 = System.getProperty("java.net.preferIpv4Stack");
      //HACK - for some reason this does not propagate from pom.xml to java. All other props do, strange...
      if (preferIpV4 == null) {                                
         System.out.println("Enforcing java.net.preferIpv4Stack");
         System.setProperty("java.net.preferIpv4Stack","true");
         preferIpV4 = System.getProperty("java.net.preferIpv4Stack");
      }
      log("java.net.preferIpv4Stack = " + preferIpV4);
      log("java.net.preferIpv6Stack = " + System.getProperty("java.net.preferIpv6Stack"));
      log("~~~~~~~~~~~~~~~~~~~~~~~~~ ENVIRONMENT INFO ~~~~~~~~~~~~~~~~~~~~~~~~~~");
   }

   private void log(String s) {
      System.out.println(s);
      log.info(s);
   }
}
