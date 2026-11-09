package org.infinispan.spring.embedded.session;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.spring.common.InfinispanTestExecutionListener;
import org.infinispan.spring.common.provider.SpringCache;
import org.infinispan.spring.common.session.AbstractInfinispanSessionRepository;
import org.infinispan.spring.common.session.AbstractInfinispanSessionRepository.InfinispanSession;
import org.infinispan.spring.common.session.InfinispanApplicationPublishedBridgeTCK;
import org.infinispan.spring.embedded.provider.BasicConfiguration;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.springframework.session.events.SessionDestroyedEvent;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(testName = "spring.embedded.session.EmbeddedApplicationPublishedBridgeTest", groups = "unit")
@ContextConfiguration(classes = BasicConfiguration.class)
@TestExecutionListeners(value = InfinispanTestExecutionListener.class, mergeMode =  TestExecutionListeners.MergeMode.MERGE_WITH_DEFAULTS)
public class EmbeddedApplicationPublishedBridgeTest extends InfinispanApplicationPublishedBridgeTCK {

   private EmbeddedCacheManager embeddedCacheManager;

   @BeforeClass
   public void beforeClass() {
      embeddedCacheManager = TestCacheManagerFactory.createCacheManager(new ConfigurationBuilder());
   }

   @AfterMethod
   public void afterMethod() {
      embeddedCacheManager.getCache().clear();
   }

   @AfterClass
   public void afterClass() {
      embeddedCacheManager.stop();
   }

   @BeforeMethod
   public void beforeMethod() throws Exception {
      super.init();
   }

   @Override
   protected SpringCache createSpringCache() {
      return new SpringCache(embeddedCacheManager.getCache());
   }

   @Override
   protected void callEviction() {
      embeddedCacheManager.getCache().getAdvancedCache().getExpirationManager().processExpiration();
   }

   @Override
   protected AbstractInfinispanSessionRepository createRepository(SpringCache springCache) throws Exception {
      InfinispanEmbeddedSessionRepository sessionRepository = new InfinispanEmbeddedSessionRepository(springCache);
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

   @Override
   public void testEventBridgeWithSessionIdChange() throws Exception {
      super.testEventBridgeWithSessionIdChange();
   }

   /**
    * An unconditional {@code Cache#remove()} of a key which is not present still raises a
    * {@link org.infinispan.notifications.cachelistener.annotation.CacheEntryRemoved} event, but
    * without an old value. The bridge must not fail and must not report a destroyed session.
    */
   public void testRemovingUnknownSessionDoesNotPublishDestroyedEvent() {
      //given
      List<Object> events = new CopyOnWriteArrayList<>();
      sessionRepository.setApplicationEventPublisher(events::add);

      //when
      embeddedCacheManager.getCache().remove("not-present");

      //then
      assertEquals(0, countDestroyedEvents(events));
   }

   /**
    * Reproduces the race in {@link AbstractInfinispanSessionRepository#deleteById(String)}: the
    * session is still readable when {@code findById} decides it is expired, but its cache entry is
    * gone by the time {@code remove()} runs. The resulting removal event carries no old value.
    */
   public void testRemovingExpiredSessionDoesNotPublishDestroyedEvent() {
      //given
      List<Object> events = new CopyOnWriteArrayList<>();
      sessionRepository.setApplicationEventPublisher(events::add);

      InfinispanSession session = sessionRepository.createSession();
      session.setMaxInactiveInterval(Duration.ofSeconds(1));
      sessionRepository.save(session);
      String sessionId = session.getId();

      //when
      eventually(() -> embeddedCacheManager.getCache().get(sessionId) == null);
      embeddedCacheManager.getCache().remove(sessionId);

      //then
      assertEquals(0, countDestroyedEvents(events));
   }

   private long countDestroyedEvents(List<Object> events) {
      return events.stream().filter(e -> e.getClass() == SessionDestroyedEvent.class).count();
   }
}
