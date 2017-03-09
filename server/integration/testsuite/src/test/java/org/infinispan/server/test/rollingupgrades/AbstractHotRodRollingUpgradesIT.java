package org.infinispan.server.test.rollingupgrades;

import javax.management.ObjectName;

import org.infinispan.arquillian.core.InfinispanResource;
import org.infinispan.arquillian.core.RemoteInfinispanServers;
import org.infinispan.arquillian.utils.MBeanServerConnectionProvider;
import org.infinispan.client.hotrod.ProtocolVersion;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.server.test.util.RemoteCacheManagerFactory;
import org.infinispan.server.test.util.RemoteInfinispanMBeans;
import org.jboss.arquillian.container.test.api.ContainerController;
import org.jboss.arquillian.test.api.ArquillianResource;
import org.junit.After;
import org.junit.Before;

/**
 * Base class for HotRod rolling upgrade tests.
 *
 * @author vjuranek
 * @since 9.0
 */
public class AbstractHotRodRollingUpgradesIT {

   static final String DEFAULT_CACHE_NAME = "default";

   @InfinispanResource
   protected RemoteInfinispanServers serverManager;

   @ArquillianResource
   protected ContainerController controller;
   
   protected RemoteCacheManagerFactory rcmFactory;

   @Before
   public void setUp() {
      rcmFactory = new RemoteCacheManagerFactory();
   }

   @After
   public void tearDown() {
      if (rcmFactory != null) {
         rcmFactory.stopManagers();
      }
      rcmFactory = null;
   }

   protected RemoteCache<Object, Object> createCache(RemoteInfinispanMBeans cacheBeans) {
      if (ProtocolVersion.parseVersion(System.getProperty("hotrod.protocol.version")) != null) {
         // we might want to test backwards compatibility as well
         // old Hot Rod protocol version was set for communication with new server
         return createCache(cacheBeans, System.getProperty("hotrod.protocol.version"));
      } else {
         return createCache(cacheBeans, ProtocolVersion.DEFAULT_PROTOCOL_VERSION.toString());
      }
   }

   protected RemoteCache<Object, Object> createCache(RemoteInfinispanMBeans cacheBeans, String protocolVersion) {
      return rcmFactory.createCache(cacheBeans, protocolVersion);
   }

   protected RemoteInfinispanMBeans createRemotes(String serverName, String managerName, String cacheName) {
      return RemoteInfinispanMBeans.create(serverManager, serverName, cacheName, managerName);
   }

   protected Object invokeOperation(MBeanServerConnectionProvider provider, String mbean, String operationName, Object[] params,
                                    String[] signature) throws Exception {
      return provider.getConnection().invoke(new ObjectName(mbean), operationName, params, signature);
   }

}
