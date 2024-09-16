package org.infinispan.server.security.authorization;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;

import javax.management.JMException;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;

import org.infinispan.commons.test.Exceptions;
import org.infinispan.server.test.api.TestUser;
import org.infinispan.server.test.core.ServerRunMode;
import org.infinispan.server.test.core.tags.Security;
import org.infinispan.server.test.junit5.InfinispanServerExtension;
import org.infinispan.server.test.junit5.InfinispanServerExtensionBuilder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

/**
 * @since 15.0
 **/
@Security
public class AuthorizationJMXIT {

   @RegisterExtension
   public static InfinispanServerExtension SERVERS =
         InfinispanServerExtensionBuilder.config("configuration/AuthorizationJMXTest.xml")
               .runMode(ServerRunMode.CONTAINER)
               .enableJMX()
               .numServers(1)
               .build();

   @Test
   public void testMonitorCanReadButCannotWrite() throws IOException, JMException {
      MBeanServerConnection jmxConnection = SERVERS.jmx().withCredentials(TestUser.MONITOR.getUser(), TestUser.MONITOR.getPassword()).get(0);
      ObjectName objectName = new ObjectName("org.infinispan:type=Cache,name=\"predefined(dist_sync)\",manager=\"default\",component=Cache");
      String cacheStatus = (String) jmxConnection.getAttribute(objectName, "cacheStatus");
      assertEquals("RUNNING", cacheStatus);
      Exceptions.expectException(SecurityException.class, "Access denied! Invalid access level for requested MBeanServer operation.",
            () -> jmxConnection.invoke(objectName, "clear", new Object[0], new String[0]));
   }

   @Test
   public void testAdminCanReadAndWrite() throws IOException, JMException {
      MBeanServerConnection jmxConnection = SERVERS.jmx().withCredentials(TestUser.ADMIN.getUser(), TestUser.ADMIN.getPassword()).get(0);
      ObjectName objectName = new ObjectName("org.infinispan:type=Cache,name=\"predefined(dist_sync)\",manager=\"default\",component=Cache");
      String cacheStatus = (String) jmxConnection.getAttribute(objectName, "cacheStatus");
      assertEquals("RUNNING", cacheStatus);
      jmxConnection.invoke(objectName, "clear", new Object[0], new String[0]);
   }

   @Test
   public void testBlockWrongCredentials() {
      Exceptions.expectException(SecurityException.class, "Authentication failed! Security Exception",
            () -> SERVERS.jmx().withCredentials(TestUser.ADMIN.getUser(), "h4X0rz").get(0));
   }

   @Test
   public void testOtherUserCannotDoAnything() {
      Exceptions.expectException(SecurityException.class, "Access denied! No entries found in the access file .*",
            () -> SERVERS.jmx().withCredentials(TestUser.APPLICATION.getUser(), TestUser.APPLICATION.getPassword()).get(0));
   }
}
