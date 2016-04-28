package org.infinispan.cli;

import static org.testng.AssertJUnit.assertEquals;
import org.infinispan.cli.connection.jmx.JMXUrl;
import org.infinispan.cli.connection.jmx.remoting.DirectJMXRemotingUrl;
import org.infinispan.cli.connection.jmx.remoting.JMXRemotingUrl;
import org.testng.annotations.Test;

@Test(groups="functional", testName="cli.JMXRemotingUrlTest")
public class JMXRemotingUrlTest {

   public void testValidJMXUrl() {
      JMXUrl jmxUrl = new DirectJMXRemotingUrl("remoting://localhost:12345");
      assertEquals("service:jmx:remoting-jmx://localhost:12345", jmxUrl.getJMXServiceURL());
   }

   public void testValidJMXUrlWithContainer() {
      JMXUrl jmxUrl = new DirectJMXRemotingUrl("remoting://localhost:12345/container");
      assertEquals("service:jmx:remoting-jmx://localhost:12345", jmxUrl.getJMXServiceURL());
      assertEquals("container", jmxUrl.getContainer());
   }

   public void testValidJMXUrlWithContainerAndCache() {
      JMXUrl jmxUrl = new DirectJMXRemotingUrl("remoting://localhost:12345/container/cache");
      assertEquals("service:jmx:remoting-jmx://localhost:12345", jmxUrl.getJMXServiceURL());
      assertEquals("container", jmxUrl.getContainer());
      assertEquals("cache", jmxUrl.getCache());
   }

   public void testValidIPV6JMXUrl() {
      JMXUrl jmxUrl = new DirectJMXRemotingUrl("remoting://[FEDC:BA98:7654:3210:FEDC:BA98:7654:3210]:12345");
      assertEquals("service:jmx:remoting-jmx://[FEDC:BA98:7654:3210:FEDC:BA98:7654:3210]:12345", jmxUrl.getJMXServiceURL());
   }

   public void testValidIPV6JMXUrlWithContainerAndCache() {
      JMXUrl jmxUrl = new DirectJMXRemotingUrl("remoting://[FEDC:BA98:7654:3210:FEDC:BA98:7654:3210]:12345/container/cache");
      assertEquals("service:jmx:remoting-jmx://[FEDC:BA98:7654:3210:FEDC:BA98:7654:3210]:12345", jmxUrl.getJMXServiceURL());
      assertEquals("container", jmxUrl.getContainer());
      assertEquals("cache", jmxUrl.getCache());
   }

   public void testEmptyJMXUrl() {
      JMXUrl jmxUrl = new DirectJMXRemotingUrl("");
      assertEquals("service:jmx:remoting-jmx://localhost:9999", jmxUrl.getJMXServiceURL());
   }

   @Test(expectedExceptions=IllegalArgumentException.class)
   public void testInvalidJMXUrl() {
      new DirectJMXRemotingUrl("hotrod://localhost:12345");
   }
}
