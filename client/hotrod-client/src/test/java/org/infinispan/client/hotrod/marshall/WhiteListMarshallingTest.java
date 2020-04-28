package org.infinispan.client.hotrod.marshall;

import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_SERIALIZED_OBJECT;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;

import java.io.Serializable;

import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.client.hotrod.test.InternalRemoteCacheManager;
import org.infinispan.client.hotrod.test.SingleHotRodServerTest;
import org.infinispan.commons.marshall.JavaSerializationMarshaller;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.data.Person;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

@Test(testName = "client.hotrod.marshall.WhiteListMarshallingTest", groups = {"functional", "smoke"} )
public class WhiteListMarshallingTest extends SingleHotRodServerTest {

   @Override
   protected RemoteCacheManager getRemoteCacheManager() {
      ConfigurationBuilder builder = HotRodClientTestingUtil.newRemoteConfigurationBuilder();
      builder.addJavaSerialWhiteList(".*Person.*").marshaller(JavaSerializationMarshaller.class);
      builder.addServer().host("127.0.0.1").port(hotrodServer.getPort());
      return new InternalRemoteCacheManager(builder.build());
   }

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      return TestCacheManagerFactory.createCacheManager(contextInitializer(), hotRodCacheConfiguration(APPLICATION_SERIALIZED_OBJECT));
   }

   @Test(expectedExceptions = HotRodClientException.class,
      expectedExceptionsMessageRegExp = ".*ISPN004034:.*")
   public void testUnsafeClassNotAllowed() {
      remoteCacheManager.getCache().put("unsafe", new UnsafeClass());
      remoteCacheManager.getCache().get("unsafe");
   }

   public void testSafeClassAllowed() {
      remoteCacheManager.getCache().put("safe", new Person());
      remoteCacheManager.getCache().get("safe");
   }

   private static final class UnsafeClass implements Serializable {
   }

}
