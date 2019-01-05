package org.infinispan.api.flags;

import java.util.EnumSet;

import org.infinispan.AdvancedCache;
import org.infinispan.cache.impl.CacheImpl;
import org.infinispan.cache.impl.DecoratedCache;
import org.infinispan.commons.util.EnumUtil;
import org.infinispan.context.Flag;
import org.testng.annotations.Test;

/**
 * @author Sanne Grinovero &lt;sanne@infinispan.org&gt; (C) 2011 Red Hat Inc.
 */
@Test(groups = "functional", testName = "api.flags.DecoratedCacheTest")
public class DecoratedCacheTest {

   public void testDecoratedCacheFlagsSet() {
      CacheImpl impl = new CacheImpl("baseCache");
      DecoratedCache decoratedCache = new DecoratedCache(impl, EnumUtil.EMPTY_BIT_SET);
      DecoratedCache nofailCache = (DecoratedCache) decoratedCache.withFlags(Flag.FAIL_SILENTLY);
      EnumSet<Flag> nofailCacheFlags = EnumUtil.enumSetOf(nofailCache.getFlagsBitSet(), Flag.class);
      assert nofailCacheFlags.contains(Flag.FAIL_SILENTLY);
      assert nofailCacheFlags.size() == 1;
      DecoratedCache asyncNoFailCache = (DecoratedCache) nofailCache.withFlags(Flag.FORCE_ASYNCHRONOUS);
      EnumSet<Flag> asyncNofailCacheFlags = EnumUtil.enumSetOf(asyncNoFailCache.getFlagsBitSet(), Flag.class);
      assert asyncNofailCacheFlags.size() == 2;
      assert asyncNofailCacheFlags.contains(Flag.FAIL_SILENTLY);
      assert asyncNofailCacheFlags.contains(Flag.FORCE_ASYNCHRONOUS);
      AdvancedCache again = asyncNoFailCache.withFlags(Flag.FAIL_SILENTLY);
      assert again == asyncNoFailCache; // as FAIL_SILENTLY was already specified
   }

}
