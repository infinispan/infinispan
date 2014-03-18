package org.infinispan.server.test.query;

import org.infinispan.arquillian.core.RemoteInfinispanServer;
import org.infinispan.arquillian.utils.MBeanServerConnectionProvider;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.marshall.ProtoStreamMarshaller;
import org.infinispan.commons.util.Util;
import org.infinispan.protostream.sampledomain.User;
import org.infinispan.protostream.sampledomain.marshallers.MarshallerRegistration;
import org.infinispan.server.test.util.RemoteCacheManagerFactory;
import org.junit.After;
import org.junit.Before;

import javax.management.ObjectName;
import java.io.IOException;
import java.io.InputStream;

import static org.infinispan.server.test.util.TestUtil.invokeOperation;

/**
 * Base class for tests for remote queries over HotRod.
 *
 * @author Adrian Nistor
 * @author Martin Gencur
 */
public abstract class RemoteQueryBaseTest {

   protected final String cacheContainerName;
   protected final String cacheName;

   protected RemoteCacheManager remoteCacheManager;
   protected RemoteCache<Integer, User> remoteCache;
   protected MBeanServerConnectionProvider jmxConnectionProvider;
   protected RemoteCacheManagerFactory rcmFactory;

   protected RemoteQueryBaseTest(String cacheContainerName, String cacheName) {
      this.cacheContainerName = cacheContainerName;
      this.cacheName = cacheName;
   }

   protected abstract RemoteInfinispanServer getServer();

   @Before
   public void setUp() throws Exception {
      jmxConnectionProvider = new MBeanServerConnectionProvider(getServer().getHotrodEndpoint().getInetAddress().getHostName(), 9999);
      rcmFactory = new RemoteCacheManagerFactory();
      ConfigurationBuilder clientBuilder = new ConfigurationBuilder();
      clientBuilder.addServer()
            .host(getServer().getHotrodEndpoint().getInetAddress().getHostName())
            .port(getServer().getHotrodEndpoint().getPort())
            .marshaller(new ProtoStreamMarshaller());
      remoteCacheManager = rcmFactory.createManager(clientBuilder);
      remoteCache = remoteCacheManager.getCache(cacheName);

      String mbean = "jboss.infinispan:type=RemoteQuery,name="
            + ObjectName.quote(cacheContainerName)
            + ",component=ProtobufMetadataManager";

      //initialize server-side serialization context via JMX
      byte[] descriptor = readClasspathResource("/sample_bank_account/bank.protobin");
      invokeOperation(jmxConnectionProvider, mbean, "registerProtofile", new Object[]{descriptor}, new String[]{byte[].class.getName()});

      //initialize client-side serialization context
      MarshallerRegistration.registerMarshallers(ProtoStreamMarshaller.getSerializationContext(remoteCacheManager));
   }

   @After
   public void tearDown() {
      if (remoteCache != null) {
         remoteCache.clear();
      }
      if (rcmFactory != null) {
         rcmFactory.stopManagers();
      }
      rcmFactory = null;
   }

   private byte[] readClasspathResource(String resourcePath) throws IOException {
      InputStream is = getClass().getResourceAsStream(resourcePath);
      try {
         return Util.readStream(is);
      } finally {
         if (is != null) {
            is.close();
         }
      }
   }
}
