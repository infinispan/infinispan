package org.infinispan.spring.remote.session;

import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_SERIALIZED_OBJECT;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.infinispan.spring.remote.AbstractRemoteCacheManagerFactory.SPRING_JAVA_SERIAL_ALLOWLIST;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.event.ClientCacheEntryCustomEvent;
import org.infinispan.commons.io.UnsignedNumeric;
import org.infinispan.commons.marshall.JavaSerializationMarshaller;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.server.hotrod.test.HotRodTestingUtil;
import org.infinispan.spring.common.InfinispanTestExecutionListener;
import org.infinispan.spring.common.provider.SpringCache;
import org.infinispan.spring.common.session.AbstractInfinispanSessionRepository;
import org.infinispan.spring.common.session.InfinispanApplicationPublishedBridgeTCK;
import org.infinispan.spring.remote.provider.BasicConfiguration;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.util.KeyValuePair;
import org.springframework.session.MapSession;
import org.springframework.session.Session;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(testName = "spring.session.RemoteApplicationPublishedBridgeTest", groups = "unit")
@ContextConfiguration(classes = BasicConfiguration.class)
@TestExecutionListeners(value = InfinispanTestExecutionListener.class,  mergeMode =  TestExecutionListeners.MergeMode.MERGE_WITH_DEFAULTS)
public class RemoteApplicationPublishedBridgeTest extends InfinispanApplicationPublishedBridgeTCK {

   private EmbeddedCacheManager embeddedCacheManager;
   private HotRodServer hotrodServer;
   private RemoteCacheManager remoteCacheManager;

   @BeforeClass
   public void beforeClass() {
      embeddedCacheManager = TestCacheManagerFactory.createCacheManager(hotRodCacheConfiguration(APPLICATION_SERIALIZED_OBJECT));
      hotrodServer = HotRodTestingUtil.startHotRodServer(embeddedCacheManager, 19723);
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.addServer().host("localhost").port(hotrodServer.getPort())
            .marshaller(new JavaSerializationMarshaller())
            .addJavaSerialAllowList(SPRING_JAVA_SERIAL_ALLOWLIST.split(","));
      remoteCacheManager = new RemoteCacheManager(builder.build());
   }

   @AfterMethod
   public void afterMethod() {
      remoteCacheManager.getCache().clear();
   }

   @AfterClass
   public void afterClass() {
      embeddedCacheManager.stop();
      remoteCacheManager.stop();
      hotrodServer.stop();
   }

   @BeforeMethod
   public void beforeMethod() throws Exception {
      super.init();
   }

   @Override
   protected SpringCache createSpringCache() {
      return new SpringCache(remoteCacheManager.getCache());
   }

   @Override
   protected void callEviction() {
      embeddedCacheManager.getCache().getAdvancedCache().getExpirationManager().processExpiration();
   }

   @Override
   protected AbstractInfinispanSessionRepository createRepository(SpringCache springCache) {
      InfinispanRemoteSessionRepository sessionRepository = new InfinispanRemoteSessionRepository(springCache);
      sessionRepository.afterPropertiesSet();
      return sessionRepository;
   }

   @Override
   public void testEventBridge() throws Exception {
      super.testEventBridge();
   }

   @Override
   public void testUnregistration() throws Exception {
      super.testUnregistration();
   }

   public void testReadEventWithoutValue() {
      RemoteApplicationPublishedBridge remoteApplicationPublishedBridge = new RemoteApplicationPublishedBridge(createSpringCache());
      String id = "1234";
      ClientCacheEntryCustomEvent<byte[]> event = new TestEvent(id);
      KeyValuePair<String, Session> keyValuePair = remoteApplicationPublishedBridge.readEvent(event);
      assertEquals(id, keyValuePair.getKey());
      assertNotNull(keyValuePair.getValue());
      MapSession value = (MapSession) keyValuePair.getValue();
      assertEquals(id, value.getId());
   }

   @Override
   public void testEventBridgeWithSessionIdChange() throws Exception {
      super.testEventBridgeWithSessionIdChange();
   }

   class TestEvent implements ClientCacheEntryCustomEvent<byte[]> {

      private final String sessionId;

      public TestEvent(String sessionId) {
         this.sessionId = sessionId;
      }

      @Override
      public byte[] getEventData() {
         RemoteCache<?, ?> cache = remoteCacheManager.getCache();
         byte[] key = cache.getDataFormat().keyToBytes(sessionId);

         int capacity = UnsignedNumeric.sizeUnsignedInt(key.length) + key.length;

         byte[] out = new byte[capacity];
         int offset = UnsignedNumeric.writeUnsignedInt(out, 0, key.length);
         System.arraycopy(key, 0, out, offset, key.length);
         return out;
      }

      @Override
      public boolean isCommandRetried() {
         return false;
      }

      @Override
      public Type getType() {
         return null;
      }
   }
}
