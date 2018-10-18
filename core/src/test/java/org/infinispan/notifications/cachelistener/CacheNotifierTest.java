package org.infinispan.notifications.cachelistener;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.hamcrest.MockitoHamcrest.argThat;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.hamcrest.CustomTypeSafeMatcher;
import org.hamcrest.Matcher;
import org.infinispan.Cache;
import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.metadata.Metadata;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.util.concurrent.CompletableFutures;
import org.infinispan.util.concurrent.IsolationLevel;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * @author Mircea Markus
 * @since 5.1
 */
@Test(groups = "functional", testName = "notifications.cachelistener.CacheNotifierTest")
public class CacheNotifierTest extends AbstractInfinispanTest {

   protected Cache<Object, Object> cache;
   protected EmbeddedCacheManager cm;

   @BeforeMethod
   public void setUp() throws Exception {
      ConfigurationBuilder c = new ConfigurationBuilder();
      c
         .transaction().transactionMode(TransactionMode.NON_TRANSACTIONAL)
         .clustering().cacheMode(CacheMode.LOCAL)
         .locking().isolationLevel(IsolationLevel.REPEATABLE_READ);
      cm = TestCacheManagerFactory.createCacheManager(c);
      cache = getCache();
      CacheNotifier mockNotifier = mock(CacheNotifier.class, i -> CompletableFutures.completedNull());
      doReturn(true).when(mockNotifier).hasListener(any());
      TestingUtil.replaceComponent(cache, CacheNotifier.class, mockNotifier, true);
   }

   protected Cache<Object, Object> getCache() {
      return cm.getCache();
   }

   @AfterMethod
   public void tearDown() throws Exception {
      TestingUtil.killCaches(cache);
      cm.stop();
   }

   @AfterClass
   public void destroyManager() {
      TestingUtil.killCacheManagers(cache.getCacheManager());
   }


   private CacheNotifier getMockNotifier(Cache cache) {
      return cache.getAdvancedCache().getComponentRegistry().getComponent(CacheNotifier.class);
   }

   protected Matcher<FlagAffectedCommand> getFlagMatcher() {
      Matcher<FlagAffectedCommand> flagAffectedCommandMatcher =
            new CustomTypeSafeMatcher<FlagAffectedCommand>("SKIP_LISTENER_NOTIFICATION") {
               @Override
               protected boolean matchesSafely(FlagAffectedCommand item) {
                  return !item.hasAnyFlag(FlagBitSets.SKIP_LISTENER_NOTIFICATION);
               }
            };
      return flagAffectedCommandMatcher;
   }


   public void testVisit() throws Exception {
      Matcher<FlagAffectedCommand> matcher = getFlagMatcher();
      initCacheData(cache, Collections.singletonMap("key", "value"));

      cache.get("key");

      verify(getMockNotifier(cache)).notifyCacheEntryVisited(eq("key"), eq("value"),
            eq(true), isA(InvocationContext.class), argThat(matcher));
      verify(getMockNotifier(cache)).notifyCacheEntryVisited(eq("key"), eq("value"),
            eq(false), isA(InvocationContext.class), argThat(matcher));
   }

   public void testRemoveData() throws Exception {
      Matcher<FlagAffectedCommand> matcher = getFlagMatcher();
      Map<String, String> data = new HashMap<>();
      data.put("key", "value");
      data.put("key2", "value2");
      initCacheData(cache, data);

      cache.remove("key2");

      verify(getMockNotifier(cache)).notifyCacheEntryRemoved(eq("key2"), eq("value2"),
            any(Metadata.class), eq(true), isA(InvocationContext.class), argThat(matcher));
      verify(getMockNotifier(cache)).notifyCacheEntryRemoved(eq("key2"), eq("value2"), any(Metadata.class), eq(false),
                                                   isA(InvocationContext.class), argThat(matcher));
   }

   public void testPutMap() throws Exception {
      Matcher<FlagAffectedCommand> matcher = getFlagMatcher();
      Map<Object, Object> data = new HashMap<Object, Object>();
      data.put("key", "value");
      data.put("key2", "value2");

      cache.putAll(data);

      expectSingleEntryCreated(cache, "key", "value", matcher);
      expectSingleEntryCreated(cache, "key2", "value2", matcher);
   }

   public void testOnlyModification() throws Exception {
      Matcher<FlagAffectedCommand> matcher = getFlagMatcher();
      initCacheData(cache, Collections.singletonMap("key", "value"));

      cache.put("key", "value2");

      verify(getMockNotifier(cache)).notifyCacheEntryModified(eq("key"), eq("value2"), any(Metadata.class), eq("value"),
            any(Metadata.class), eq(true), isA(InvocationContext.class), argThat(matcher));
      verify(getMockNotifier(cache)).notifyCacheEntryModified(eq("key"), eq("value2"), any(Metadata.class), eq("value"),
            any(Metadata.class), eq(false), isA(InvocationContext.class), argThat(matcher));

      cache.put("key", "value2");

      verify(getMockNotifier(cache)).notifyCacheEntryModified(eq("key"), eq("value2"), any(Metadata.class), eq("value2"),
            any(Metadata.class), eq(true), isA(InvocationContext.class), argThat(matcher));

      verify(getMockNotifier(cache)).notifyCacheEntryModified(eq("key"), eq("value2"), any(Metadata.class), eq("value2"),
            any(Metadata.class), eq(false), isA(InvocationContext.class), argThat(matcher));
   }

   public void testReplaceNotification() throws Exception {
      Matcher<FlagAffectedCommand> matcher = getFlagMatcher();
      initCacheData(cache, Collections.singletonMap("key", "value"));

      cache.replace("key", "value", "value2");

      verify(getMockNotifier(cache)).notifyCacheEntryModified(eq("key"), eq("value2"), any(Metadata.class), eq("value"),
            any(Metadata.class), eq(true), isA(InvocationContext.class), argThat(matcher));
      verify(getMockNotifier(cache)).notifyCacheEntryModified(eq("key"), eq("value2"), any(Metadata.class), eq("value"),
            any(Metadata.class), eq(false), isA(InvocationContext.class), argThat(matcher));
   }

   public void testReplaceNoNotificationOnNoChange() throws Exception {
      initCacheData(cache, Collections.singletonMap("key", "value"));

      cache.replace("key", "value2", "value3");

      Matcher<FlagAffectedCommand> matcher = getFlagMatcher();
      verify(getMockNotifier(cache), never()).notifyCacheEntryModified(eq("key"), eq("value3"), any(Metadata.class), eq("value3"),
            any(Metadata.class), eq(true), any(InvocationContext.class), argThat(matcher));
      verify(getMockNotifier(cache), never()).notifyCacheEntryModified(eq("key"), eq("value3"), any(Metadata.class), eq("value3"),
            any(Metadata.class), eq(false), any(InvocationContext.class), argThat(matcher));
   }

   public void testNonexistentVisit() throws Exception {
      cache.get("doesNotExist");
   }

   public void testNonexistentRemove() throws Exception {
      cache.remove("doesNotExist");
   }

   public void testCreation() throws Exception {
      creation(cache, getFlagMatcher());
   }

   private void creation(Cache<Object, Object> cache, Matcher<FlagAffectedCommand> matcher) {
      cache.put("key", "value");
      expectSingleEntryCreated(cache, "key", "value", matcher);
   }

   private void initCacheData(Cache cache, Map<String, String> data) {
      cache.putAll(data);
      verify(getMockNotifier(cache), atLeastOnce()).notifyCacheEntryCreated(any(),
            any(), any(Metadata.class), anyBoolean(),
            isA(InvocationContext.class), getExpectedPutMapCommand());
   }

   protected PutMapCommand getExpectedPutMapCommand() {
      return isA(PutMapCommand.class);
   }

   private void expectSingleEntryCreated(Cache cache, Object key, Object value,
                                         Matcher<FlagAffectedCommand> matcher) {
      verify(getMockNotifier(cache))
            .notifyCacheEntryCreated(eq(key), eq(value), any(Metadata.class), eq(true), any(InvocationContext.class),
                                     argThat(matcher));
      verify(getMockNotifier(cache))
            .notifyCacheEntryCreated(eq(key), eq(value), any(Metadata.class), eq(false), any(InvocationContext.class),
                                     argThat(matcher));
   }
}
