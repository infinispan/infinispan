package org.infinispan.server.jmx;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import javax.management.AttributeNotFoundException;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanException;
import javax.management.MBeanServerConnection;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.ReflectionException;

import org.infinispan.server.test.core.ServerRunMode;
import org.infinispan.server.test.junit4.InfinispanServerRule;
import org.infinispan.server.test.junit4.InfinispanServerRuleBuilder;
import org.infinispan.server.test.junit4.InfinispanServerTestMethodRule;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

public class JmxConnectionIT {

   @ClassRule
   public static InfinispanServerRule SERVERS =
         InfinispanServerRuleBuilder.config("configuration/JMXServerTest.xml")
               .runMode(ServerRunMode.CONTAINER)
               .enableJMX()
               .numServers(2)
               .build();

   @Rule
   public InfinispanServerTestMethodRule SERVER_TEST = new InfinispanServerTestMethodRule(SERVERS);

   @Test
   public void testJmxClusterSize() throws IOException, AttributeNotFoundException, MBeanException, ReflectionException,
         InstanceNotFoundException, MalformedObjectNameException {
      MBeanServerConnection connection = SERVERS.getServerDriver().getJmxConnection(0);
      ObjectName cacheManagerName = new ObjectName(
            String.format("%s:type=CacheManager,name=\"%s\",component=CacheManager", "org.infinispan", "default"));
      String clusterMembers = (String) connection.getAttribute(cacheManagerName, "clusterMembers");
      assertEquals(2, clusterMembers.split(",").length);
   }
}
