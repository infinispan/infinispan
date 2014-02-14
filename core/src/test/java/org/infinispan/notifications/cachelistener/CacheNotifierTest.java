package org.infinispan.notifications.cachelistener;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.isA;
import static org.mockito.Matchers.isNull;
import static org.mockito.Mockito.*;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.manager.CacheContainer;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.TransactionMode;
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

   private Cache<Object, Object> cache;
   private CacheNotifier mockNotifier;
   private CacheNotifier origNotifier;
   private CacheContainer cm;
   private AdvancedCache<Object, Object> skipListenerCache;

   @BeforeMethod
   public void setUp() throws Exception {
      ConfigurationBuilder c = new ConfigurationBuilder();
      c
         .transaction().transactionMode(TransactionMode.NON_TRANSACTIONAL)
         .clustering().cacheMode(CacheMode.LOCAL)
         .locking().isolationLevel(IsolationLevel.REPEATABLE_READ);
      cm = TestCacheManagerFactory.createCacheManager(c);

      cache = cm.getCache();
      skipListenerCache = cm.getCache().getAdvancedCache().withFlags(Flag.SKIP_LISTENER_NOTIFICATION);
      mockNotifier = mock(CacheNotifier.class);
      origNotifier = TestingUtil.replaceComponent(cache, CacheNotifier.class, mockNotifier, true);
   }

   @AfterMethod
   public void tearDown() throws Exception {
      TestingUtil.replaceComponent(cache, CacheNotifier.class, origNotifier, true);
      TestingUtil.killCaches(cache);
      cm.stop();
   }

   @AfterClass
   public void destroyManager() {
      TestingUtil.killCacheManagers(cache.getCacheManager());
   }

   public void testVisit() throws Exception {
      visit(cache, new SkipListenerFlagMatcher(false));
   }

   public void testSkipVisit() throws Exception {
      visit(skipListenerCache, new SkipListenerFlagMatcher(true));
   }

   private void visit(Cache<Object, Object> cache, Matcher<FlagAffectedCommand> matcher) {
      initCacheData(Collections.singletonMap("key", "value"));

      cache.get("key");

      verify(mockNotifier).notifyCacheEntryVisited(eq("key"), eq("value"),
            eq(true), isA(InvocationContext.class), argThat(matcher));
      verify(mockNotifier).notifyCacheEntryVisited(eq("key"), eq("value"),
            eq(false), isA(InvocationContext.class), argThat(matcher));
   }

   public void testRemoveData() throws Exception {
      removeData(cache, new SkipListenerFlagMatcher(false));
   }

   public void testSkipRemoveData() throws Exception {
      removeData(skipListenerCache, new SkipListenerFlagMatcher(true));
   }

   private void removeData(Cache<Object, Object> cache, Matcher<FlagAffectedCommand> matcher) {
      Map<String, String> data = new HashMap<String, String>();
      data.put("key", "value");
      data.put("key2", "value2");
      initCacheData(data);

      cache.remove("key2");

      verify(mockNotifier).notifyCacheEntryRemoved(eq("key2"), eq("value2"),
            eq("value2"), eq(true), isA(InvocationContext.class), argThat(matcher));
      verify(mockNotifier).notifyCacheEntryRemoved(eq("key2"), isNull(),
            eq("value2"), eq(false), isA(InvocationContext.class), argThat(matcher));
   }

   public void testPutMap() throws Exception {
      putMap(cache, new SkipListenerFlagMatcher(false));
   }

   public void testSkipPutMap() throws Exception {
      putMap(skipListenerCache, new SkipListenerFlagMatcher(true));
   }

   private void putMap(Cache<Object, Object> cache, Matcher<FlagAffectedCommand> matcher) {
      Map<Object, Object> data = new HashMap<Object, Object>();
      data.put("key", "value");
      data.put("key2", "value2");

      cache.putAll(data);

      expectSingleEntryCreated("key", "value", matcher);
      expectSingleEntryCreated("key2", "value2", matcher);
   }

   public void testOnlyModification() throws Exception {
      onlyModifications(cache, new SkipListenerFlagMatcher(false));
   }

   public void testSkipOnlyModification() throws Exception {
      onlyModifications(skipListenerCache, new SkipListenerFlagMatcher(true));
   }

   private void onlyModifications(Cache<Object, Object> cache, Matcher<FlagAffectedCommand> matcher) {
      initCacheData(Collections.singletonMap("key", "value"));

      cache.put("key", "value2");

      verify(mockNotifier).notifyCacheEntryModified(eq("key"), eq("value"),
            eq(false), eq(true), isA(InvocationContext.class), argThat(matcher));

      cache.put("key", "value2");

      verify(mockNotifier).notifyCacheEntryModified(eq("key"), eq("value2"),
            eq(false), eq(true), isA(InvocationContext.class), argThat(matcher));

      verify(mockNotifier, times(2)).notifyCacheEntryModified(eq("key"), eq("value2"),
            eq(false), eq(false), isA(InvocationContext.class), argThat(matcher));
   }

   public void testReplaceNotification() throws Exception {
      replaceNotification(cache, new SkipListenerFlagMatcher(false));
   }

   public void testSkipReplaceNotification() throws Exception {
      replaceNotification(skipListenerCache, new SkipListenerFlagMatcher(true));
   }

   private void replaceNotification(Cache<Object, Object> cache,
         Matcher<FlagAffectedCommand> matcher) {
      initCacheData(Collections.singletonMap("key", "value"));

      cache.replace("key", "value", "value2");

      verify(mockNotifier).notifyCacheEntryModified(eq("key"), eq("value"),
            eq(false), eq(true), isA(InvocationContext.class), argThat(matcher));
      verify(mockNotifier).notifyCacheEntryModified(eq("key"), eq("value2"),
            eq(false), eq(false), isA(InvocationContext.class), argThat(matcher));
   }

   public void testReplaceNoNotificationOnNoChange() throws Exception {
      initCacheData(Collections.singletonMap("key", "value"));

      cache.replace("key", "value2", "value3");

      verify(mockNotifier, never()).notifyCacheEntryModified(eq("key"), eq("value2"),
            eq(false), eq(true), any(InvocationContext.class),
            argThat(new SkipListenerFlagMatcher(false)));
      verify(mockNotifier, never()).notifyCacheEntryModified(eq("key"), eq("value3"),
            eq(false), eq(false), any(InvocationContext.class),
            argThat(new SkipListenerFlagMatcher(false)));
   }

   public void testNonexistentVisit() throws Exception {
      cache.get("doesNotExist");
   }

   public void testNonexistentRemove() throws Exception {
      cache.remove("doesNotExist");
   }

   public void testCreation() throws Exception {
      creation(cache, new SkipListenerFlagMatcher(false));
   }

   public void testSkipCreation() throws Exception {
      creation(skipListenerCache, new SkipListenerFlagMatcher(true));
   }

   private void creation(Cache<Object, Object> cache, Matcher<FlagAffectedCommand> matcher) {
      cache.put("key", "value");
      expectSingleEntryCreated("key", "value", matcher);
   }

   private void initCacheData(Map<String, String> data) {
      cache.putAll(data);
      verify(mockNotifier, atLeastOnce()).notifyCacheEntryCreated(anyObject(),
            anyObject(), anyBoolean(), isA(InvocationContext.class),
            isA(PutMapCommand.class));
      verify(mockNotifier, atLeastOnce()).notifyCacheEntryModified(anyObject(),
            anyObject(), eq(true), anyBoolean(), isA(InvocationContext.class),
            isA(PutMapCommand.class));
   }

   private void expectSingleEntryCreated(Object key, Object value,
         Matcher<FlagAffectedCommand> matcher) {
      verify(mockNotifier).notifyCacheEntryCreated(eq(key), isNull(), eq(true),
            isA(InvocationContext.class), argThat(matcher));
      verify(mockNotifier).notifyCacheEntryCreated(eq(key), eq(value), eq(false),
            isA(InvocationContext.class), argThat(matcher));
   }

   private static class SkipListenerFlagMatcher
         extends BaseMatcher<FlagAffectedCommand> {

      private final boolean hasFlag;

      private SkipListenerFlagMatcher(boolean hasFlag) {
         this.hasFlag = hasFlag;
      }

      @Override
      public boolean matches(Object item) {
         boolean expected = item instanceof FlagAffectedCommand;
         boolean isSkipListener = ((FlagAffectedCommand) item).hasFlag(Flag.SKIP_LISTENER_NOTIFICATION);

         if (hasFlag)
            return expected && isSkipListener;
         else
            return expected && !isSkipListener;
      }

      @Override
      public void describeTo(Description description) {
         // no-op
      }

   }

}
