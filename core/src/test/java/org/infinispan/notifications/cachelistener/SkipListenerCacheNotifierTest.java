package org.infinispan.notifications.cachelistener;

import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
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
      return new BaseMatcher<FlagAffectedCommand>() {
         @Override
         public boolean matches(Object o) {
            boolean expected = o instanceof FlagAffectedCommand;
            boolean isSkipListener = ((FlagAffectedCommand) o).hasAnyFlag(FlagBitSets.SKIP_LISTENER_NOTIFICATION);
            return expected && isSkipListener;
         }

         @Override
         public void describeTo(Description description) {
         }
      };
   }
}
