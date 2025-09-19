package org.infinispan.spring.remote.session;

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;

import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.marshall.JavaSerializationMarshaller;
import org.infinispan.commons.marshall.ProtoStreamMarshaller;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.server.hotrod.test.HotRodTestingUtil;
import org.infinispan.spring.common.InfinispanTestExecutionListener;
import org.infinispan.spring.common.provider.SpringCache;
import org.infinispan.spring.common.session.AbstractInfinispanSessionRepository;
import org.infinispan.spring.common.session.InfinispanSessionRepositoryTCK;
import org.infinispan.spring.remote.provider.BasicConfiguration;
import org.infinispan.spring.remote.provider.SpringRemoteCacheManager;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;

@Test(testName = "spring.session.InfinispanRemoteSessionRepositoryTest", groups = "functional")
@ContextConfiguration(classes = BasicConfiguration.class)
@TestExecutionListeners(value = InfinispanTestExecutionListener.class,  mergeMode =  TestExecutionListeners.MergeMode.MERGE_WITH_DEFAULTS)
public class InfinispanRemoteSessionRepositoryTest extends InfinispanSessionRepositoryTCK {

   private EmbeddedCacheManager embeddedCacheManager;
   private HotRodServer hotrodServer;
   private RemoteCacheManager remoteCacheManager;
   private SpringRemoteCacheManager springRemoteCacheManager;

   @Factory
   public Object[] factory() {
      return new Object[]{
            new InfinispanRemoteSessionRepositoryTest().mediaType(MediaType.APPLICATION_SERIALIZED_OBJECT),
            };
   }

   @BeforeClass
   public void beforeClass() {
      org.infinispan.configuration.cache.ConfigurationBuilder cacheConfiguration = hotRodCacheConfiguration(mediaType);
      embeddedCacheManager = TestCacheManagerFactory.createCacheManager(cacheConfiguration);
      hotrodServer = HotRodTestingUtil.startHotRodServer(embeddedCacheManager, 19723);
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.addServer().host("localhost").port(hotrodServer.getPort());
      if (mediaType.equals(MediaType.APPLICATION_SERIALIZED_OBJECT)) {
         builder.marshaller(JavaSerializationMarshaller.class);
      } else {
         builder.marshaller(ProtoStreamMarshaller.class);
      }
      remoteCacheManager = new RemoteCacheManager(builder.build());
      springRemoteCacheManager = new SpringRemoteCacheManager(remoteCacheManager);
   }

   @AfterMethod
   public void afterMethod() {
      remoteCacheManager.getCache().clear();
   }

   @AfterClass
   public void afterClass() {
      springRemoteCacheManager.stop();
      hotrodServer.stop();
      embeddedCacheManager.stop();
   }

   @BeforeMethod
   public void beforeMethod() throws Exception {
      super.init();
   }

   @Override
   protected SpringCache createSpringCache() {
      return springRemoteCacheManager.getCache("");
   }

   @Override
   protected AbstractInfinispanSessionRepository createRepository(SpringCache springCache) {
      InfinispanRemoteSessionRepository sessionRepository = new InfinispanRemoteSessionRepository(springCache);
      sessionRepository.afterPropertiesSet();
      return sessionRepository;
   }

   @Test(expectedExceptions = NullPointerException.class)
   @Override
   public void testThrowingExceptionOnNullSpringCache() throws Exception {
      super.testThrowingExceptionOnNullSpringCache();
   }

   @Override
   public void testCreatingSession() throws Exception {
      super.testCreatingSession();
   }

   @Override
   public void testSavingNewSession() throws Exception {
      super.testSavingNewSession();
   }

   @Override
   public void testDeletingSession() throws Exception {
      super.testDeletingSession();
   }

   @Override
   public void testEvictingSession() throws Exception {
      super.testEvictingSession();
   }

   @Override
   public void testUpdatingTTLOnAccessingData() throws Exception {
      super.testUpdatingTTLOnAccessingData();
   }

}
