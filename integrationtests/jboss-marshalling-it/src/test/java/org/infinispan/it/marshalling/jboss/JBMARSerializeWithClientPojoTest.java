package org.infinispan.it.marshalling.jboss;

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;

import java.io.ObjectInput;
import java.io.ObjectOutput;

import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.client.hotrod.test.InternalRemoteCacheManager;
import org.infinispan.client.hotrod.test.SingleHotRodServerTest;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.marshall.SerializeWith;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.jboss.marshalling.commons.GenericJBossMarshaller;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "client.hotrod.marshall.JBMARSerializeWithClientPojoTest")
public class JBMARSerializeWithClientPojoTest extends SingleHotRodServerTest {

   @Override
   protected EmbeddedCacheManager createCacheManager() {
      GlobalConfigurationBuilder globalBuilder = new GlobalConfigurationBuilder();
      globalBuilder.serialization().allowList().addClasses(UserPojo.class, UserPojo.Externalizer.class);

      org.infinispan.configuration.cache.ConfigurationBuilder builder = hotRodCacheConfiguration();
      builder.encoding().key().mediaType(MediaType.APPLICATION_OBJECT_TYPE);
      builder.encoding().value().mediaType(MediaType.APPLICATION_OBJECT_TYPE);
      return TestCacheManagerFactory.createCacheManager(globalBuilder, builder);
   }

   @Override
   protected RemoteCacheManager getRemoteCacheManager() {
      ConfigurationBuilder builder = HotRodClientTestingUtil.newRemoteConfigurationBuilder();
      builder.addJavaSerialAllowList(UserPojo.class.getName()).marshaller(GenericJBossMarshaller.class);
      builder.addServer().host("127.0.0.1").port(hotrodServer.getPort());
      return new InternalRemoteCacheManager(builder.build());
   }

   @Test
   public void testSerializeWithPojoMarshallable() {
      remoteCacheManager.getCache().put(1, new UserPojo());
   }

   @SerializeWith(UserPojo.Externalizer.class)
   public static final class UserPojo {
      public static class Externalizer implements org.infinispan.commons.marshall.Externalizer<UserPojo> {
         @Override
         public void writeObject(ObjectOutput output, UserPojo object) {
         }

         @Override
         public UserPojo readObject(ObjectInput input) {
            return new UserPojo();
         }
      }
   }
}
