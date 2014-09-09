package org.infinispan.notifications.cachelistener.cluster;

import org.infinispan.Cache;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.distribution.BlockingInterceptor;
import org.infinispan.distribution.MagicKey;
import org.infinispan.interceptors.EntryWrappingInterceptor;
import org.infinispan.notifications.cachelistener.event.CacheEntryCreatedEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryModifiedEvent;
import org.infinispan.notifications.cachelistener.event.Event;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.test.TestingUtil;
import org.infinispan.tx.dld.ControlledRpcManager;
import org.testng.annotations.Test;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.testng.AssertJUnit.*;

/**
 * Cluster listener test to make sure that when include current state is enabled that listeners still work properly
 * in a replicated cache
 *
 * @author wburns
 * @since 7.0
 */
@Test(groups = "functional", testName = "notifications.cachelistener.cluster.ClusterListenerReplInitialStateTest")
public class ClusterListenerReplInitialStateTest extends ClusterListenerReplTest {
   @Override
   protected ClusterListener listener() {
      return new ClusterListenerWithIncludeCurrentState();
   }
}
