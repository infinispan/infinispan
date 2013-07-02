package org.infinispan.cli;

import static org.testng.AssertJUnit.assertEquals;

import org.infinispan.cli.connection.jmx.JMXUrl;
import org.infinispan.cli.connection.jmx.rmi.JMXRMIUrl;
import org.testng.annotations.Test;

@Test(groups="functional", testName="cli.JMXRMIUrlTest")
public class JMXRMIUrlTest {

   public void testValidJMXUrl() {
      JMXUrl jmxUrl = new JMXRMIUrl("jmx://localhost:12345");
      assertEquals("service:jmx:rmi:///jndi/rmi://localhost:12345/jmxrmi", jmxUrl.getJMXServiceURL());
   }

   public void testValidJMXUrlWithContainer() {
      JMXUrl jmxUrl = new JMXRMIUrl("jmx://localhost:12345/container");
      assertEquals("service:jmx:rmi:///jndi/rmi://localhost:12345/jmxrmi", jmxUrl.getJMXServiceURL());
      assertEquals("container", jmxUrl.getContainer());
   }

   public void testValidJMXUrlWithContainerAndCache() {
      JMXUrl jmxUrl = new JMXRMIUrl("jmx://localhost:12345/container/cache");
      assertEquals("service:jmx:rmi:///jndi/rmi://localhost:12345/jmxrmi", jmxUrl.getJMXServiceURL());
      assertEquals("container", jmxUrl.getContainer());
      assertEquals("cache", jmxUrl.getCache());
   }

   public void testValidIPV6JMXUrl() {
      JMXUrl jmxUrl = new JMXRMIUrl("jmx://[FEDC:BA98:7654:3210:FEDC:BA98:7654:3210]:12345");
      assertEquals("service:jmx:rmi:///jndi/rmi://[FEDC:BA98:7654:3210:FEDC:BA98:7654:3210]:12345/jmxrmi", jmxUrl.getJMXServiceURL());
   }

   public void testValidIPV6JMXUrlWithContainerAndCache() {
      JMXUrl jmxUrl = new JMXRMIUrl("jmx://[FEDC:BA98:7654:3210:FEDC:BA98:7654:3210]:12345/container/cache");
      assertEquals("service:jmx:rmi:///jndi/rmi://[FEDC:BA98:7654:3210:FEDC:BA98:7654:3210]:12345/jmxrmi", jmxUrl.getJMXServiceURL());
      assertEquals("container", jmxUrl.getContainer());
      assertEquals("cache", jmxUrl.getCache());
   }

   @Test(expectedExceptions=IllegalArgumentException.class)
   public void testInvalidJMXUrl() {
      new JMXRMIUrl("hotrod://localhost:12345");
   }
}
