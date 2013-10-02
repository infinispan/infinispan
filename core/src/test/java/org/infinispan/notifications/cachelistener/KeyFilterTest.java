package org.infinispan.notifications.cachelistener;

import org.infinispan.Cache;
import org.infinispan.commons.equivalence.AnyEquivalence;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.NonTxInvocationContext;
import org.infinispan.interceptors.locking.ClusteringDependentLogic;
import org.infinispan.notifications.KeyFilter;
import org.infinispan.notifications.cachelistener.event.CacheEntryCreatedEvent;
import org.infinispan.notifications.cachelistener.event.Event;
import org.infinispan.test.AbstractInfinispanTest;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;

@Test(testName = "notifications.cachelistener.KeyFilterTest", groups = "unit")
public class KeyFilterTest extends AbstractInfinispanTest {
   CacheNotifierImpl n;
   Cache mockCache;
   CacheListener cl;
   InvocationContext ctx;

   @BeforeMethod
   public void setUp() {
      KeyFilter kf = new KeyFilter() {
         @Override
         public boolean accept(Object key) {
            return key.toString().equals("accept");
         }
      };

      n = new CacheNotifierImpl();
      mockCache = mock(Cache.class);
      n.injectDependencies(mockCache, new ClusteringDependentLogic.LocalLogic(), null);
      cl = new CacheListener();
      n.start();
      n.addListener(cl, kf);
      ctx = new NonTxInvocationContext(AnyEquivalence.getInstance());
   }

   public void testFilters() {
      n.notifyCacheEntryCreated("reject", null, true, ctx, null);
      n.notifyCacheEntryCreated("reject", "v1", false, ctx, null);

      assert !cl.isReceivedPost();
      assert !cl.isReceivedPre();
      assert cl.getInvocationCount() == 0;

      n.notifyCacheEntryCreated("accept", null, true, ctx, null);
      n.notifyCacheEntryCreated("accept", "v1", false, ctx, null);

      assert cl.isReceivedPost();
      assert cl.isReceivedPre();
      assert cl.getInvocationCount() == 2;
      assert cl.getEvents().get(0).getCache() == mockCache;
      assert cl.getEvents().get(0).getType() == Event.Type.CACHE_ENTRY_CREATED;
      assert ((CacheEntryCreatedEvent) cl.getEvents().get(0)).getKey().equals("accept");
      assert ((CacheEntryCreatedEvent) cl.getEvents().get(0)).getValue() == null;
      assert cl.getEvents().get(1).getCache() == mockCache;
      assert cl.getEvents().get(1).getType() == Event.Type.CACHE_ENTRY_CREATED;
      assert ((CacheEntryCreatedEvent) cl.getEvents().get(1)).getKey().equals("accept");
      assert ((CacheEntryCreatedEvent) cl.getEvents().get(1)).getValue().equals("v1");
   }
}
