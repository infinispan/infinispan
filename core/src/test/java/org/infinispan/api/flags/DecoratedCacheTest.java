package org.infinispan.api.flags;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
      CacheImpl<String, String> impl = new CacheImpl<>("baseCache");
      DecoratedCache<String, String> decoratedCache = new DecoratedCache<>(impl, EnumUtil.EMPTY_BIT_SET);
      DecoratedCache<String, String> nofailCache = (DecoratedCache<String, String>) decoratedCache.withFlags(Flag.FAIL_SILENTLY);
      EnumSet<Flag> nofailCacheFlags = EnumUtil.enumSetOf(nofailCache.getFlagsBitSet(), Flag.class);
      assertTrue(nofailCacheFlags.contains(Flag.FAIL_SILENTLY));
      assertEquals(1, nofailCacheFlags.size());
      DecoratedCache<String, String> asyncNoFailCache = (DecoratedCache<String, String>) nofailCache.withFlags(Flag.FORCE_ASYNCHRONOUS);
      EnumSet<Flag> asyncNofailCacheFlags = EnumUtil.enumSetOf(asyncNoFailCache.getFlagsBitSet(), Flag.class);
      assertEquals(2, asyncNofailCacheFlags.size());
      assertTrue(asyncNofailCacheFlags.contains(Flag.FAIL_SILENTLY));
      assertTrue(asyncNofailCacheFlags.contains(Flag.FORCE_ASYNCHRONOUS));
      AdvancedCache<String, String> again = asyncNoFailCache.withFlags(Flag.FAIL_SILENTLY);
      assertSame(again, asyncNoFailCache); // as FAIL_SILENTLY was already specified
   }
}
