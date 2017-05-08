package org.infinispan.notifications.cachelistener;

import org.hamcrest.CustomTypeSafeMatcher;
import org.hamcrest.Matcher;
import org.infinispan.Cache;
import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.context.Flag;
import org.infinispan.context.impl.FlagBitSets;
import org.testng.annotations.Test;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
@Test(groups = "functional", testName = "notifications.cachelistener.SkipListenerCacheNotifierTest")
public class SkipListenerCacheNotifierTest extends CacheNotifierTest {
   @Override
   protected Cache<Object, Object> getCache() {
      return cm.getCache().getAdvancedCache().withFlags(Flag.SKIP_LISTENER_NOTIFICATION);
   }

   @Override
   protected Matcher<FlagAffectedCommand> getFlagMatcher() {
      return new CustomTypeSafeMatcher<FlagAffectedCommand>("") {
         @Override
         protected boolean matchesSafely(FlagAffectedCommand item) {
            return item.hasAnyFlag(FlagBitSets.SKIP_LISTENER_NOTIFICATION);
         }
      };
   }
}
