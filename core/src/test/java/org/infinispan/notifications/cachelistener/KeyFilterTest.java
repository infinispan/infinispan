package org.infinispan.notifications.cachelistener;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;

import org.infinispan.cache.impl.EncoderCache;
import org.infinispan.commands.CancellationService;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.impl.InternalEntryFactory;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.NonTxInvocationContext;
import org.infinispan.distribution.ch.KeyPartitioner;
import org.infinispan.encoding.DataConversion;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.impl.BasicComponentRegistry;
import org.infinispan.filter.KeyFilter;
import org.infinispan.interceptors.AsyncInterceptor;
import org.infinispan.interceptors.AsyncInterceptorChain;
import org.infinispan.interceptors.locking.ClusteringDependentLogic;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.notifications.cachelistener.cluster.ClusterEventManager;
import org.infinispan.notifications.cachelistener.event.CacheEntryCreatedEvent;
import org.infinispan.notifications.cachelistener.event.Event;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.MockBasicComponentRegistry;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(testName = "notifications.cachelistener.KeyFilterTest", groups = "unit")
public class KeyFilterTest extends AbstractInfinispanTest {
   CacheNotifierImpl n;
   EncoderCache mockCache;
   CacheListener cl;
   InvocationContext ctx;

   @BeforeMethod
   public void setUp() {
      KeyFilter kf = key -> key.toString().equals("accept");

      n = new CacheNotifierImpl();
      mockCache = mock(EncoderCache.class);
      EmbeddedCacheManager cacheManager = mock(EmbeddedCacheManager.class);
      when(mockCache.getCacheManager()).thenReturn(cacheManager);
      when(mockCache.getAdvancedCache()).thenReturn(mockCache);
      when(mockCache.getKeyDataConversion()).thenReturn(DataConversion.DEFAULT_KEY);
      when(mockCache.getValueDataConversion()).thenReturn(DataConversion.DEFAULT_VALUE);
      when(mockCache.getStatus()).thenReturn(ComponentStatus.RUNNING);
      AsyncInterceptorChain asyncInterceptorChain = mock(AsyncInterceptorChain.class);
      when(mockCache.getAsyncInterceptorChain()).thenReturn(asyncInterceptorChain);
      when(asyncInterceptorChain.getInterceptors()).thenReturn(Collections.singletonList(mock(AsyncInterceptor.class)));
      ComponentRegistry componentRegistry = mock(ComponentRegistry.class);
      when(mockCache.getComponentRegistry()).thenReturn(componentRegistry);
      MockBasicComponentRegistry mockRegistry = new MockBasicComponentRegistry();
      when(componentRegistry.getComponent(BasicComponentRegistry.class)).thenReturn(mockRegistry);
      mockRegistry.registerMocks(RpcManager.class, StreamingMarshaller.class, CancellationService.class,
                                 CommandsFactory.class);
      ClusteringDependentLogic.LocalLogic cdl = new ClusteringDependentLogic.LocalLogic();
      Configuration config = new ConfigurationBuilder().build();
      cdl.init(null, config, mock(KeyPartitioner.class));
      TestingUtil.inject(n, mockCache, cdl, config, mockRegistry,
                         mock(InternalEntryFactory.class), mock(ClusterEventManager.class), mock(KeyPartitioner.class));
      cl = new CacheListener();
      n.start();
      n.addListener(cl, kf);
      ctx = new NonTxInvocationContext(null);
   }

   public void testFilters() {
      n.notifyCacheEntryCreated("reject", "v1", null, true, ctx, null);
      n.notifyCacheEntryCreated("reject", "v1", null, false, ctx, null);

      assert !cl.isReceivedPost();
      assert !cl.isReceivedPre();
      assert cl.getInvocationCount() == 0;

      n.notifyCacheEntryCreated("accept", "v1", null, true, ctx, null);
      n.notifyCacheEntryCreated("accept", "v1", null, false, ctx, null);

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
