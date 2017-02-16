package org.infinispan.notifications.cachelistener;

import static org.mockito.Matchers.isNull;

import org.hamcrest.Matcher;
import org.hamcrest.core.IsNull;
import org.infinispan.Cache;
import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryVisited;
import org.infinispan.notifications.cachelistener.event.CacheEntryVisitedEvent;
import org.testng.annotations.Test;

/**
 * @author Mircea Markus
 * @since 5.1
 */
@Test(groups = "functional", testName = "notifications.cachelistener.SimpleCacheNotifierTest")
public class SimpleCacheNotifierTest extends CacheNotifierTest {
   @Override
   protected Cache<Object, Object> getCache() {
      cm.defineConfiguration("simple", new ConfigurationBuilder().read(cm.getDefaultCacheConfiguration())
            .clustering().simpleCache(true)
            .jmxStatistics().available(false)
            .build());
      Cache cache = cm.getCache("simple");
      // without any listeners the notifications are ignored
      cache.addListener(new DummyListener());
      return cache;
   }

   @Override
   protected Matcher<FlagAffectedCommand> getFlagMatcher() {
      return new IsNull<>();
   }

   @Override
   protected PutMapCommand getExpectedPutMapCommand() {
      return isNull(PutMapCommand.class);
   }

   @Listener
   private class DummyListener {
      @CacheEntryVisited
      public void onVisited(CacheEntryVisitedEvent e) {}
   }
}
