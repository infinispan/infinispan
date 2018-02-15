package org.infinispan.client.hotrod.marshall;

import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.client.hotrod.test.InternalRemoteCacheManager;
import org.infinispan.client.hotrod.test.SingleHotRodServerTest;
import org.infinispan.test.data.Person;
import org.testng.annotations.Test;

import java.io.Serializable;

@Test(testName = "client.hotrod.marshall.WhiteListMarshallingTest", groups = {"functional", "smoke"} )
public class WhiteListMarshallingTest extends SingleHotRodServerTest {

   @Override
   protected RemoteCacheManager getRemoteCacheManager() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.addJavaSerialWhiteList(".*Person.*");
      builder.addServer().host("127.0.0.1").port(hotrodServer.getPort());
      return new InternalRemoteCacheManager(builder.build());
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
