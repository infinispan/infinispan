/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License, v. 2.1.
 * This program is distributed in the hope that it will be useful, but WITHOUT A
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License,
 * v.2.1 along with this distribution; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA  02110-1301, USA.
 */
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
