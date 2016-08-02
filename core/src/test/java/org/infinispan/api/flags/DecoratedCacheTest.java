package org.infinispan.api.flags;

import org.infinispan.AdvancedCache;
import org.infinispan.cache.impl.CacheImpl;
import org.infinispan.cache.impl.DecoratedCache;
import org.infinispan.context.Flag;
import org.testng.annotations.Test;

/**
 * @author Sanne Grinovero <sanne@infinispan.org> (C) 2011 Red Hat Inc.
 */
@Test(groups = "functional", testName = "api.flags.DecoratedCacheTest")
public class DecoratedCacheTest {

   public void testDecoratedCacheFlagsSet() {
      CacheImpl impl = new CacheImpl("baseCache");
      DecoratedCache decoratedCache = new DecoratedCache(impl);
      DecoratedCache nofailCache = (DecoratedCache) decoratedCache.withFlags(Flag.FAIL_SILENTLY);
      assert nofailCache.getFlags().contains(Flag.FAIL_SILENTLY);
      assert nofailCache.getFlags().size() == 1;
      DecoratedCache asyncNoFailCache = (DecoratedCache) nofailCache.withFlags(Flag.FORCE_ASYNCHRONOUS);
      assert asyncNoFailCache.getFlags().size() == 2;
      assert asyncNoFailCache.getFlags().contains(Flag.FAIL_SILENTLY);
      assert asyncNoFailCache.getFlags().contains(Flag.FORCE_ASYNCHRONOUS);
      AdvancedCache again = asyncNoFailCache.withFlags(Flag.FAIL_SILENTLY);
      assert again == asyncNoFailCache; // as FAIL_SILENTLY was already specified
   }

}
