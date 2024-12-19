package org.infinispan.test;

import static org.infinispan.persistence.manager.PersistenceManager.AccessMode.BOTH;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.fail;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.security.Principal;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.management.JMException;
import javax.management.MBeanInfo;
import javax.management.MBeanOperationInfo;
import javax.management.MBeanParameterInfo;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.security.auth.Subject;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.cache.impl.AbstractDelegatingCache;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.TimeoutException;
import org.infinispan.commons.api.Lifecycle;
import org.infinispan.commons.jdkspecific.CallerId;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.marshall.ProtoStreamMarshaller;
import org.infinispan.commons.test.Exceptions;
import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.IntSets;
import org.infinispan.commons.util.Version;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.commons.util.concurrent.CompletionStages;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.impl.InternalDataContainer;
import org.infinispan.context.Flag;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.distribution.LocalizedCacheTopology;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.distribution.ch.ConsistentHashFactory;
import org.infinispan.distribution.ch.KeyPartitioner;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.factories.impl.BasicComponentRegistry;
import org.infinispan.factories.impl.ComponentRef;
import org.infinispan.factories.impl.TestComponentAccessors;
import org.infinispan.interceptors.AsyncInterceptor;
import org.infinispan.interceptors.AsyncInterceptorChain;
import org.infinispan.jmx.CacheJmxRegistration;
import org.infinispan.jmx.CacheManagerJmxRegistration;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.lifecycle.ModuleLifecycle;
import org.infinispan.manager.CacheContainer;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.marshall.persistence.impl.MarshalledEntryUtil;
import org.infinispan.marshall.persistence.impl.PersistenceMarshallerImpl;
import org.infinispan.metadata.EmbeddedMetadata;
import org.infinispan.metadata.Metadata;
import org.infinispan.notifications.cachelistener.CacheNotifier;
import org.infinispan.notifications.cachelistener.CacheNotifierImpl;
import org.infinispan.notifications.cachemanagerlistener.CacheManagerNotifier;
import org.infinispan.notifications.cachemanagerlistener.CacheManagerNotifierImpl;
import org.infinispan.persistence.dummy.DummyInMemoryStore;
import org.infinispan.persistence.manager.PersistenceManager;
import org.infinispan.persistence.manager.PersistenceManagerImpl;
import org.infinispan.persistence.spi.AdvancedLoadWriteStore;
import org.infinispan.persistence.spi.CacheLoader;
import org.infinispan.persistence.spi.CacheWriter;
import org.infinispan.persistence.spi.MarshallableEntry;
import org.infinispan.persistence.spi.NonBlockingStore;
import org.infinispan.persistence.support.DelegatingNonBlockingStore;
import org.infinispan.persistence.support.DelegatingPersistenceManager;
import org.infinispan.persistence.support.NonBlockingStoreAdapter;
import org.infinispan.persistence.support.SegmentPublisherWrapper;
import org.infinispan.persistence.support.SingleSegmentPublisher;
import org.infinispan.persistence.support.WaitDelegatingNonBlockingStore;
import org.infinispan.protostream.ProtobufUtil;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.registry.InternalCacheRegistry;
import org.infinispan.remoting.inboundhandler.PerCacheInboundInvocationHandler;
import org.infinispan.remoting.transport.AbstractDelegatingTransport;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.remoting.transport.jgroups.JGroupsAddress;
import org.infinispan.remoting.transport.jgroups.JGroupsTransport;
import org.infinispan.security.AuthorizationPermission;
import org.infinispan.security.GroupPrincipal;
import org.infinispan.security.actions.SecurityActions;
import org.infinispan.security.impl.SecureCacheImpl;
import org.infinispan.statetransfer.StateTransferManagerImpl;
import org.infinispan.topology.CacheTopology;
import org.infinispan.transaction.impl.TransactionTable;
import org.infinispan.util.DependencyGraph;
import org.infinispan.util.concurrent.locks.LockManager;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.jgroups.JChannel;
import org.jgroups.MergeView;
import org.jgroups.View;
import org.jgroups.ViewId;
import org.jgroups.protocols.DELAY;
import org.jgroups.protocols.DISCARD;
import org.jgroups.protocols.TP;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.MutableDigest;
import org.reactivestreams.Publisher;
import org.testng.AssertJUnit;

import io.reactivex.rxjava3.core.Flowable;
import jakarta.transaction.Status;
import jakarta.transaction.TransactionManager;

public class TestingUtil {
   private static final Log log = LogFactory.getLog(TestingUtil.class);
   private static final Random random = new Random();
   private static final int SHORT_TIMEOUT_MILLIS = Integer.getInteger("infinispan.test.shortTimeoutMillis", 500);
   private static final ScheduledExecutorService timeoutExecutor;

   static {
      ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1, r -> {
         Thread t = new Thread(r);
         t.setDaemon(true);
         t.setName("test-timeout-thread");
         return t;
      });
      executor.setRemoveOnCancelPolicy(true);
      timeoutExecutor = executor;
   }

   public static void assertNotDone(CompletionStage<?> completionStage) {
      sleepThread(50);
      assertFalse(completionStage.toCompletableFuture().isDone());
   }

   public static void assertNotDone(Future<?> completionStage) {
      sleepThread(50);
      assertFalse(completionStage.isDone());
   }

   public static <T> CompletableFuture<T> orTimeout(CompletableFuture<T> f, long timeout, TimeUnit timeUnit, Executor executor) {
      ScheduledFuture<?> scheduled = timeoutExecutor.schedule(() -> {
         // Don't run anything on the timeout thread
         executor.execute(() -> f.completeExceptionally(new TimeoutException("Timed out!")));
      }, timeout, timeUnit);
      f.whenComplete((v, t) -> scheduled.cancel(false));
      return f;
   }

   public static <T> CompletionStage<T> startAsync(Callable<CompletionStage<T>> action, Executor executor) {
      return CompletableFutures.completedNull().thenComposeAsync(ignored -> Exceptions.unchecked(action), executor);
   }

   public static <T> CompletionStage<T> sequence(CompletionStage<?> first, Callable<CompletionStage<T>> second) {
      return first.thenCompose(ignored -> Exceptions.unchecked(second));
   }

   public static <T> CompletionStage<T> sequenceAsync(CompletionStage<?> first, Callable<CompletionStage<T>> second, Executor executor) {
      return first.thenComposeAsync(ignored -> Exceptions.unchecked(second), executor);
   }

   public static <T> T join(CompletionStage<? extends T> stage) throws ExecutionException, InterruptedException, java.util.concurrent.TimeoutException {
      return stage.toCompletableFuture().get(10, TimeUnit.SECONDS);
   }

   /**
    * Should be used by tests for a timeout when they need to wait for that timeout to expire.
    *
    * <p>Can be changed with the {@code org.infinispan.test.shortTimeoutMillis} system property.</p>
    */
   public static long shortTimeoutMillis() {
      return SHORT_TIMEOUT_MILLIS;
   }

   /**
    * Simulates a node crash, discarding all the messages from/to this node and then stopping the caches.
    */
   public static void crashCacheManagers(EmbeddedCacheManager... cacheManagers) {
      for (EmbeddedCacheManager cm : cacheManagers) {
         JChannel channel = extractJChannel(cm);
         try {
            DISCARD discard = new DISCARD();
            discard.discardAll(true);
            channel.getProtocolStack().insertProtocol(discard, ProtocolStack.Position.ABOVE, TP.class);
         } catch (Exception e) {
            log.warn("Problems inserting discard", e);
            throw new RuntimeException(e);
         }
         View view = View.create(channel.getAddress(), 100, channel.getAddress());
         ((GMS) channel.getProtocolStack().findProtocol(GMS.class)).installView(view);
      }
      killCacheManagers(cacheManagers);
   }

   public static void installNewView(EmbeddedCacheManager... members) {
      installNewView(Stream.of(members).map(EmbeddedCacheManager::getAddress), members);
   }

   public static void installNewView(Function<EmbeddedCacheManager, JChannel> channelRetriever, EmbeddedCacheManager... members) {
      installNewView(Stream.of(members).map(EmbeddedCacheManager::getAddress), channelRetriever, members);
   }

   public static void installNewView(Stream<Address> members, EmbeddedCacheManager... where) {
      installNewView(members, TestingUtil::extractJChannel, where);
   }

   public static JChannel extractJChannel(EmbeddedCacheManager ecm) {
      Transport transport = extractGlobalComponent(ecm, Transport.class);
      while (!(transport instanceof JGroupsTransport)) {
         if (Proxy.isProxyClass(transport.getClass())) {
            // Unwrap proxies created by the StateSequencer
            transport = extractField(extractField(transport, "h"), "wrappedInstance");
         } else if (transport instanceof AbstractDelegatingTransport) {
            transport = ((AbstractDelegatingTransport) transport).getDelegate();
         } else {
            throw new IllegalStateException("Unable to obtain a JGroupsTransport instance from " + transport + " on " + ecm.getAddress());
         }
      }
      return ((JGroupsTransport) transport).getChannel();
   }

   public static void installNewView(Stream<Address> members, Function<EmbeddedCacheManager, JChannel> channelRetriever, EmbeddedCacheManager... where) {
      List<org.jgroups.Address> viewMembers = members.map(a -> ((JGroupsAddress) a).getJGroupsAddress()).collect(Collectors.toList());

      List<View> previousViews = new ArrayList<>(where.length);
      // Compute the merge digest, without it nodes would request the retransmission of all messages
      // Including those that were removed by STABLE earlier
      MutableDigest digest = new MutableDigest(viewMembers.toArray(new org.jgroups.Address[0]));
      for (EmbeddedCacheManager ecm : where) {
         GMS gms = channelRetriever.apply(ecm).getProtocolStack().findProtocol(GMS.class);
         previousViews.add(gms.view());
         digest.merge(gms.getDigest());
      }

      long viewId = previousViews.stream().mapToLong(view -> view.getViewId().getId()).max().orElse(0) + 1;
      View newView;
      if (previousViews.stream().allMatch(view -> view.getMembers().containsAll(viewMembers))) {
         newView = View.create(viewMembers.get(0), viewId, viewMembers.toArray(new org.jgroups.Address[0]));
      } else {
         newView = new MergeView(new ViewId(viewMembers.get(0), viewId), viewMembers, previousViews);
      }

      log.trace("Before installing new view:" + viewMembers);
      for (EmbeddedCacheManager ecm : where) {
         ((GMS) channelRetriever.apply(ecm).getProtocolStack().findProtocol(GMS.class)).installView(newView, digest);
      }
   }

   public static String wrapXMLWithSchema(String schema, String xml) {
      StringBuilder sb = new StringBuilder();
      sb.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
      sb.append("<infinispan xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" ");
      sb.append("xsi:schemaLocation=\"urn:infinispan:config:");
      sb.append(schema);
      sb.append(" https://infinispan.org/schemas/infinispan-config-");
      sb.append(schema);
      sb.append(".xsd\" xmlns=\"urn:infinispan:config:");
      sb.append(schema);
      sb.append("\">\n");
      sb.append(xml);
      sb.append("</infinispan>");
      return sb.toString();
   }

   public static String wrapXMLWithSchema(String xml) {
      return wrapXMLWithSchema(Version.getSchemaVersion(), xml);
   }

   public static String wrapXMLWithoutSchema(String xml) {
      StringBuilder sb = new StringBuilder();
      sb.append("<infinispan>\n");
      sb.append(xml);
      sb.append("</infinispan>");
      return sb.toString();
   }

   /**
    * Extracts the value of a field in a given target instance using reflection, able to extract private fields as
    * well.
    *
    * @param target    object to extract field from
    * @param fieldName name of field to extract
    * @return field value
    */
   public static <T> T extractField(Object target, String fieldName) {
      return extractField(target.getClass(), target, fieldName);
   }

   public static void replaceField(Object newValue, String fieldName, Object owner, Class<?> baseType) {
      Field field;
      try {
         field = baseType.getDeclaredField(fieldName);
         field.setAccessible(true);
         field.set(owner, newValue);
      } catch (Exception e) {
         throw new RuntimeException(e);//just to simplify exception handling
      }
   }

   public static <T> void replaceField(Object owner, String fieldName, Function<T, T> func) {
      replaceField(owner.getClass(), owner, fieldName, func);
   }

   public static <T> void replaceField(Class<?> baseType, Object owner, String fieldName, Function<T, T> func) {
      Field field;
      try {
         field = baseType.getDeclaredField(fieldName);
         field.setAccessible(true);
         Object prevValue = field.get(owner);
         Object newValue = func.apply((T) prevValue);
         field.set(owner, newValue);
      } catch (Exception e) {
         throw new RuntimeException(e);//just to simplify exception handling
      }
   }

   public static <T> T extractField(Class<?> type, Object target, String fieldName) {
      while (true) {
         Field field;
         try {
            field = type.getDeclaredField(fieldName);
            field.setAccessible(true);
            return (T) field.get(target);
         } catch (Exception e) {
            if (type == Object.class) {
               throw new RuntimeException(e);
            } else {
               // try with superclass!!
               type = type.getSuperclass();
            }
         }
      }
   }

   public static <T extends AsyncInterceptor> T findInterceptor(Cache<?, ?> cache,
                                                                Class<T> interceptorToFind) {
      return extractInterceptorChain(cache).findInterceptorExtending(interceptorToFind);
   }

   public static int getSegmentForKey(Object key, Cache<?, ?> cache) {
      KeyPartitioner keyPartitioner = extractComponent(cache, KeyPartitioner.class);
      return keyPartitioner.getSegment(key);
   }

   /**
    * Waits until pendingCH() is null on all caches, currentCH.getMembers() contains all caches provided as the param
    * and all segments have numOwners owners.
    */
   public static void waitForNoRebalance(Cache... caches) {
      final int REBALANCE_TIMEOUT_SECONDS = 60; //Needs to be rather large to prevent sporadic failures on CI
      final long giveup = System.nanoTime() + TimeUnit.SECONDS.toNanos(REBALANCE_TIMEOUT_SECONDS);
      int zeroCapacityCaches = 0;
      for (Cache<?, ?> c : caches) {
         if (c.getCacheConfiguration().clustering().hash().capacityFactor() == 0f ||
               c.getCacheManager().getCacheManagerConfiguration().isZeroCapacityNode()) {
            zeroCapacityCaches++;
         }
      }
      for (Cache<?, ?> c : caches) {
         c = unwrapSecureCache(c);
         int numOwners = c.getCacheConfiguration().clustering().hash().numOwners();
         DistributionManager distributionManager = c.getAdvancedCache().getDistributionManager();
         Address cacheAddress = c.getAdvancedCache().getRpcManager().getAddress();
         CacheTopology cacheTopology;
         while (true) {
            cacheTopology = distributionManager.getCacheTopology();
            boolean rebalanceInProgress;
            boolean chContainsAllMembers;
            boolean currentChIsBalanced;
            if (cacheTopology != null) {
               rebalanceInProgress = cacheTopology.getPhase() != CacheTopology.Phase.NO_REBALANCE;
               ConsistentHash currentCH = cacheTopology.getCurrentCH();
               ConsistentHashFactory chf = StateTransferManagerImpl.pickConsistentHashFactory(
                     extractGlobalConfiguration(c.getCacheManager()), c.getCacheConfiguration());

               chContainsAllMembers = currentCH.getMembers().size() == caches.length;
               currentChIsBalanced = true;

               int actualNumOwners = Math.min(numOwners, currentCH.getMembers().size() - zeroCapacityCaches);
               for (int i = 0; i < currentCH.getNumSegments(); i++) {
                  if (currentCH.locateOwnersForSegment(i).size() < actualNumOwners) {
                     currentChIsBalanced = false;
                     break;
                  }
               }

               // We need to check that the topologyId > 1 to account for nodes restarting
               if (chContainsAllMembers && !rebalanceInProgress && cacheTopology.getTopologyId() > 1) {
                  rebalanceInProgress = !chf.rebalance(currentCH).equals(currentCH);
               }
               if (chContainsAllMembers && !rebalanceInProgress && currentChIsBalanced)
                  break;
            } else {
               rebalanceInProgress = false;
               chContainsAllMembers = false;
               currentChIsBalanced = true;
            }

            //System.out.printf("Cache %s Address %s cacheTopology %s rebalanceInProgress %s chContainsAllMembers %s, currentChIsBalanced %s\n", c.getName(), cacheAddress, cacheTopology, rebalanceInProgress, chContainsAllMembers, currentChIsBalanced);

            if (System.nanoTime() - giveup > 0) {
               String message;
               if (!chContainsAllMembers) {
                  Address[] addresses = new Address[caches.length];
                  for (int i = 0; i < caches.length; i++) {
                     addresses[i] = caches[i].getCacheManager().getAddress();
                  }
                  message = String.format("Cache %s timed out waiting for rebalancing to complete on node %s, " +
                              "expected member list is %s, current member list is %s!", c.getName(), cacheAddress,
                        Arrays.toString(addresses), cacheTopology == null ? "N/A" : cacheTopology.getCurrentCH().getMembers());
               } else {
                  message = String.format("Cache %s timed out waiting for rebalancing to complete on node %s, " +
                              "current topology is %s. rebalanceInProgress=%s, currentChIsBalanced=%s", c.getName(),
                        c.getCacheManager().getAddress(), cacheTopology, rebalanceInProgress, currentChIsBalanced);
               }
               log.error(message);
               throw new RuntimeException(message);
            }

            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(100));
         }
         log.trace("Node " + cacheAddress + " finished state transfer, has topology " + cacheTopology);
      }
   }

   public static void waitForNoRebalanceAcrossManagers(EmbeddedCacheManager... managers) {
      int numberOfManagers = managers.length;
      assert numberOfManagers > 0;
      Set<String> testCaches = getInternalAndUserCacheNames(managers[0]);
      log.debugf("waitForNoRebalance with managers %s, for caches %s", Arrays.toString(managers), testCaches);

      for (String cacheName : testCaches) {
         ArrayList<Cache<?, ?>> caches = new ArrayList<>(numberOfManagers);
         for (EmbeddedCacheManager manager : managers) {
            Cache<?, ?> cache = manager.getCache(cacheName, false);
            if (cache != null) {
               caches.add(cache);
            }
         }

         TestingUtil.waitForNoRebalance(caches.toArray(new Cache[0]));
      }
   }

   public static Set<String> getInternalAndUserCacheNames(EmbeddedCacheManager cacheManager) {
      Set<String> testCaches = new HashSet<>(cacheManager.getCacheNames());
      testCaches.addAll(getInternalCacheNames(cacheManager));
      return testCaches;
   }

   public static Set<String> getInternalCacheNames(CacheContainer container) {
      return extractGlobalComponentRegistry(container).getComponent(InternalCacheRegistry.class).getInternalCacheNames();
   }

   public static void waitForTopologyPhase(List<Address> expectedMembers, CacheTopology.Phase phase,
                                           Cache<?, ?>... caches) {
      final int TOPOLOGY_TIMEOUT_SECONDS = 60; //Needs to be rather large to prevent sporadic failures on CI
      final long giveup = System.nanoTime() + TimeUnit.SECONDS.toNanos(TOPOLOGY_TIMEOUT_SECONDS);
      for (Cache<?, ?> c : caches) {
         c = unwrapSecureCache(c);
         DistributionManager distributionManager = c.getAdvancedCache().getDistributionManager();
         while (true) {
            CacheTopology cacheTopology = distributionManager.getCacheTopology();
            boolean allMembersExist = cacheTopology != null && cacheTopology.getMembers().containsAll(expectedMembers);
            boolean isCorrectPhase = cacheTopology != null && cacheTopology.getPhase() == phase;
            if (allMembersExist && isCorrectPhase) break;

            if (System.nanoTime() - giveup > 0) {
               String message = String.format("Timed out waiting for a CacheTopology to be installed with members %s and phase %s. Current topology=%s",
                     expectedMembers, phase, cacheTopology);
               log.error(message);
               throw new RuntimeException(message);
            }

            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(100));
         }
      }
   }

   private static Cache<?, ?> unwrapSecureCache(Cache<?, ?> c) {
      if (c instanceof SecureCacheImpl) {
         c = extractField(SecureCacheImpl.class, c, "delegate");
      }
      return c;
   }

   public static void waitForNoRebalance(Collection<? extends Cache> caches) {
      waitForNoRebalance(caches.toArray(new Cache[0]));
   }

   /**
    * Loops, continually calling {@link #areCacheViewsComplete(Cache[])} until it either returns true or
    * <code>timeout</code> ms have elapsed.
    *
    * @param caches  caches which must all have consistent views
    * @param timeout max number of ms to loop
    * @throws RuntimeException if <code>timeout</code> ms have elapse without all caches having the same number of
    *                          members.
    */
   public static void blockUntilViewsReceived(Cache<?, ?>[] caches, long timeout) {
      long failTime = System.currentTimeMillis() + timeout;

      while (System.currentTimeMillis() < failTime) {
         sleepThread(100);
         if (areCacheViewsComplete(caches)) {
            return;
         }
      }

      viewsTimedOut(caches);
   }

   private static void viewsTimedOut(Cache<?, ?>[] caches) {
      CacheContainer[] cacheContainers = new CacheContainer[caches.length];
      for (int i = 0; i < caches.length; i++) {
         cacheContainers[i] = caches[i].getCacheManager();
      }
      viewsTimedOut(cacheContainers);
   }

   private static void viewsTimedOut(CacheContainer[] cacheContainers) {
      int length = cacheContainers.length;
      List<View> incompleteViews = new ArrayList<>(length);
      for (CacheContainer cacheContainer : cacheContainers) {
         EmbeddedCacheManager cm = (EmbeddedCacheManager) cacheContainer;
         if (cm.getMembers().size() != cacheContainers.length) {
            incompleteViews.add(extractJChannel(cm).getView());
            log.warnf("Manager %s has an incomplete view: %s", cm.getAddress(), cm.getMembers());
         }
      }

      throw new TimeoutException(String.format(
            "Timed out before caches had complete views.  Expected %d members in each view.  Views are as follows: %s",
            cacheContainers.length, incompleteViews));
   }

   public static void blockUntilViewsReceivedInt(Cache<?, ?>[] caches, long timeout) throws InterruptedException {
      long failTime = System.currentTimeMillis() + timeout;

      while (System.currentTimeMillis() < failTime) {
         sleepThreadInt(100, null);
         if (areCacheViewsComplete(caches)) {
            return;
         }
      }

      viewsTimedOut(caches);
   }

   /**
    * Version of blockUntilViewsReceived that uses varargs
    */
   public static void blockUntilViewsReceived(long timeout, Cache<?, ?>... caches) {
      blockUntilViewsReceived(caches, timeout);
   }

   /**
    * Version of blockUntilViewsReceived that throws back any interruption
    */
   public static void blockUntilViewsReceivedInt(long timeout, Cache<?, ?>... caches) throws InterruptedException {
      blockUntilViewsReceivedInt(caches, timeout);
   }

   /**
    * Version of blockUntilViewsReceived that uses varargsa and cache managers
    */
   public static void blockUntilViewsReceived(long timeout, CacheContainer... cacheContainers) {
      blockUntilViewsReceived(timeout, true, cacheContainers);
   }

   /**
    * Waits for the given members to be removed from the cluster. The difference between this and {@link
    * #blockUntilViewsReceived(long, org.infinispan.manager.CacheContainer...)} methods(s) is that it does not barf if
    * more than expected members is in the cluster - this is because we expect to start with a grater number fo members
    * than we eventually expect. It will barf though, if the number of members is not the one expected but only after
    * the timeout expires.
    */
   public static void blockForMemberToFail(long timeout, CacheContainer... cacheContainers) {
      blockUntilViewsReceived(timeout, false, cacheContainers);
      areCacheViewsComplete(true, cacheContainers);
   }

   public static void blockUntilViewsReceived(long timeout, boolean barfIfTooManyMembers, CacheContainer... cacheContainers) {
      long failTime = System.currentTimeMillis() + timeout;

      while (System.currentTimeMillis() < failTime) {
         if (areCacheViewsComplete(barfIfTooManyMembers, cacheContainers)) {
            return;
         }
         sleepThread(100);
      }

      viewsTimedOut(cacheContainers);
   }

   /**
    * An overloaded version of {@link #blockUntilViewsReceived(long, Cache[])} that allows for 'shrinking' clusters.
    * I.e., the usual method barfs if there are more members than expected.  This one takes a param
    * (barfIfTooManyMembers) which, if false, will NOT barf but will wait until the cluster 'shrinks' to the desired
    * size.  Useful if in tests, you kill a member and want to wait until this fact is known across the cluster.
    */
   public static void blockUntilViewsReceived(long timeout, boolean barfIfTooManyMembers, Cache<?, ?>... caches) {
      long failTime = System.currentTimeMillis() + timeout;

      while (System.currentTimeMillis() < failTime) {
         sleepThread(100);
         if (areCacheViewsComplete(caches, barfIfTooManyMembers)) {
            return;
         }
      }

      viewsTimedOut(caches);
   }

   /**
    * Loops, continually calling {@link #areCacheViewsComplete(Cache[])} until it either returns true or
    * <code>timeout</code> ms have elapsed.
    *
    * @param groupSize number of caches expected in the group
    * @param timeout   max number of ms to loop
    * @throws RuntimeException if <code>timeout</code> ms have elapse without all caches having the same number of
    *                          members.
    */
   public static void blockUntilViewReceived(Cache<?, ?> cache, int groupSize, long timeout) {
      blockUntilViewReceived(cache, groupSize, timeout, true);
   }

   /**
    * Loops, continually calling {@link #areCacheViewsComplete(Cache[])} until
    * it either returns true or a default timeout has elapsed.
    *
    * @param groupSize number of caches expected in the group
    */
   public static void blockUntilViewReceived(Cache<?, ?> cache, int groupSize) {
      // Default 10 seconds
      blockUntilViewReceived(cache, groupSize, 10000, true);
   }

   public static void blockUntilViewReceived(Cache<?, ?> cache, int groupSize, long timeout, boolean barfIfTooManyMembersInView) {
      long failTime = System.currentTimeMillis() + timeout;

      while (System.currentTimeMillis() < failTime) {
         sleepThread(100);
         EmbeddedCacheManager cacheManager = cache.getCacheManager();
         if (isCacheViewComplete(cacheManager.getMembers(), cacheManager.getAddress(), groupSize, barfIfTooManyMembersInView)) {
            return;
         }
      }

      throw new RuntimeException(String.format(
            "Timed out before cache had %d members.  View is %s",
            groupSize, cache.getCacheManager().getMembers()));
   }

   /**
    * Checks each cache to see if the number of elements in the array returned by {@link
    * EmbeddedCacheManager#getMembers()} matches the size of the <code>caches</code> parameter.
    *
    * @param caches caches that should form a View
    * @return <code>true</code> if all caches have <code>caches.length</code> members; false otherwise
    * @throws IllegalStateException if any of the caches have MORE view members than caches.length
    */
   public static boolean areCacheViewsComplete(Cache<?, ?>[] caches) {
      return areCacheViewsComplete(caches, true);
   }

   public static boolean areCacheViewsComplete(Cache<?, ?>[] caches, boolean barfIfTooManyMembers) {
      int memberCount = caches.length;

      for (Cache<?, ?> cache : caches) {
         EmbeddedCacheManager cacheManager = cache.getCacheManager();
         if (!isCacheViewComplete(cacheManager.getMembers(), cacheManager.getAddress(), memberCount, barfIfTooManyMembers)) {
            return false;
         }
      }

      return true;
   }

   public static boolean areCacheViewsComplete(boolean barfIfTooManyMembers, CacheContainer... cacheContainers) {
      if (cacheContainers == null) throw new NullPointerException("Cache Manager array is null");
      int memberCount = cacheContainers.length;

      for (CacheContainer cacheContainer : cacheContainers) {
         EmbeddedCacheManager cacheManager = (EmbeddedCacheManager) cacheContainer;
         if (!isCacheViewComplete(cacheManager.getMembers(), cacheManager.getAddress(), memberCount, barfIfTooManyMembers)) {
            return false;
         }
      }

      return true;
   }

   public static boolean isCacheViewComplete(Cache<?, ?> c, int memberCount) {
      EmbeddedCacheManager cacheManager = c.getCacheManager();
      return isCacheViewComplete(cacheManager.getMembers(), cacheManager.getAddress(), memberCount, true);
   }

   public static boolean isCacheViewComplete(List<Address> members, Address address, int memberCount, boolean barfIfTooManyMembers) {
      if (members == null || memberCount > members.size()) {
         return false;
      } else if (memberCount < members.size()) {
         if (barfIfTooManyMembers) {
            // This is an exceptional condition
            StringBuilder sb = new StringBuilder("Cache at address ");
            sb.append(address);
            sb.append(" had ");
            sb.append(members.size());
            sb.append(" members; expecting ");
            sb.append(memberCount);
            sb.append(". Members were (");
            for (int j = 0; j < members.size(); j++) {
               if (j > 0) {
                  sb.append(", ");
               }
               sb.append(members.get(j));
            }
            sb.append(')');

            throw new IllegalStateException(sb.toString());
         } else return false;
      }

      return true;
   }

   /**
    * This method blocks until the given caches have a view of whose size
    * matches the desired value. This method is particularly useful for
    * discovering that members have been split, or that they have joined back
    * again.
    *
    * @param timeout       max number of milliseconds to block for
    * @param finalViewSize desired final view size
    * @param caches        caches representing current, or expected members in the cluster.
    */
   public static void blockUntilViewsChanged(long timeout, int finalViewSize, Cache<?, ?>... caches) {
      blockUntilViewsChanged(caches, timeout, finalViewSize);
   }

   private static void blockUntilViewsChanged(Cache<?, ?>[] caches, long timeout, int finalViewSize) {
      long failTime = System.currentTimeMillis() + timeout;

      while (System.currentTimeMillis() < failTime) {
         sleepThread(100);
         if (areCacheViewsChanged(caches, finalViewSize)) {
            return;
         }
      }

      List<List<Address>> allViews = new ArrayList<>(caches.length);
      for (Cache<?, ?> cache : caches) {
         allViews.add(cache.getCacheManager().getMembers());
      }

      throw new RuntimeException(String.format(
            "Timed out before caches had changed views (%s) to contain %d members",
            allViews, finalViewSize));
   }

   private static boolean areCacheViewsChanged(Cache<?, ?>[] caches, int finalViewSize) {
      for (Cache<?, ?> cache : caches) {
         EmbeddedCacheManager cacheManager = cache.getCacheManager();
         if (!isCacheViewChanged(cacheManager.getMembers(), finalViewSize)) {
            return false;
         }
      }

      return true;
   }

   private static boolean isCacheViewChanged(List<Address> members, int finalViewSize) {
      return !(members == null || finalViewSize != members.size());
   }


   /**
    * Puts the current thread to sleep for the desired number of ms, suppressing any exceptions.
    *
    * @param sleeptime number of ms to sleep
    */
   public static void sleepThread(long sleeptime) {
      sleepThread(sleeptime, null);
   }

   public static void sleepThread(long sleeptime, String messageOnInterrupt) {
      try {
         Thread.sleep(sleeptime);
      } catch (InterruptedException ie) {
         if (messageOnInterrupt != null)
            log.error(messageOnInterrupt);
      }
   }

   private static void sleepThreadInt(long sleeptime, String messageOnInterrupt) throws InterruptedException {
      try {
         Thread.sleep(sleeptime);
      } catch (InterruptedException ie) {
         if (messageOnInterrupt != null)
            log.error(messageOnInterrupt);
         throw ie;
      }
   }

   public static void sleepRandom(int maxTime) {
      sleepThread(random.nextInt(maxTime));
   }

   public static void killCacheManagers(CacheContainer... cacheContainers) {
      EmbeddedCacheManager[] cms = new EmbeddedCacheManager[cacheContainers.length];
      for (int i = 0; i < cacheContainers.length; i++) cms[i] = (EmbeddedCacheManager) cacheContainers[i];
      killCacheManagers(cms);
   }

   public static void killCacheManagers(EmbeddedCacheManager... cacheManagers) {
      killCacheManagers(Arrays.asList(cacheManagers));
   }

   public static void killCacheManagers(List<? extends EmbeddedCacheManager> cacheManagers) {
      // Stop the managers in reverse order to prevent each of them from becoming coordinator in turn
      if (cacheManagers != null) {
         for (int i = cacheManagers.size() - 1; i >= 0; i--) {
            EmbeddedCacheManager cm = cacheManagers.get(i);
            try {
               if (cm != null) {
                  SecurityActions.stopManager(cm);
               }
            } catch (Throwable e) {
               log.warn("Problems killing cache manager " + cm, e);
            }
         }
      }
   }

   public static void clearContent(EmbeddedCacheManager... cacheManagers) {
      clearContent(Arrays.asList(cacheManagers));
   }

   public static void clearContent(List<? extends EmbeddedCacheManager> cacheManagers) {
      if (cacheManagers != null) {
         for (EmbeddedCacheManager cm : cacheManagers) {
            try {
               clearContent(cm);
            } catch (Throwable e) {
               log.warn("Problems clearing cache manager " + cm, e);
            }
         }
      }
   }

   public static void clearContent(EmbeddedCacheManager cacheContainer) {
      if (cacheContainer != null && cacheContainer.getStatus().allowInvocations()) {
         Set<Cache<?, ?>> runningCaches = getRunningCaches(cacheContainer);
         for (Cache<?, ?> cache : runningCaches) {
            clearRunningTx(cache);
         }

         if (!cacheContainer.getStatus().allowInvocations()) return;

         for (Cache<?, ?> cache : runningCaches) {
            try {
               clearCacheLoader(cache);
               removeInMemoryData(cache);
            } catch (Exception e) {
               log.errorf(e, "Failed to clear cache %s after test", cache);
            }
         }
      }
   }

   private static Set<String> getOrderedCacheNames(EmbeddedCacheManager cacheContainer) {
      Set<String> caches = new LinkedHashSet<>();
      try {
         DependencyGraph<String> graph =
               TestingUtil.extractGlobalComponent(cacheContainer, DependencyGraph.class,
                     KnownComponentNames.CACHE_DEPENDENCY_GRAPH);
         caches.addAll(graph.topologicalSort());
      } catch (Exception ignored) {
      }
      return caches;
   }

   private static Set<Cache<?, ?>> getRunningCaches(EmbeddedCacheManager cacheContainer) {
      if (cacheContainer == null || !cacheContainer.getStatus().allowInvocations())
         return Collections.emptySet();

      Set<String> running = new LinkedHashSet<>(getOrderedCacheNames(cacheContainer));
      extractGlobalComponent(cacheContainer, InternalCacheRegistry.class).filterPrivateCaches(running);
      running.addAll(cacheContainer.getCacheNames());
      extractGlobalConfiguration(cacheContainer).defaultCacheName().ifPresent(running::add);

      HashSet<Cache<?, ?>> runningCaches = new LinkedHashSet<>();
      for (String cacheName : running) {
         Cache<?, ?> cache;
         try {
            cache = cacheContainer.getCache(cacheName, false);
         } catch (CacheException ignoreCache) {
            // Ignore caches that have not started correctly
            continue;
         }
         if (cache != null && cache.getStatus().allowInvocations())
            runningCaches.add(cache);
      }
      return runningCaches;
   }

   public static GlobalConfiguration extractGlobalConfiguration(EmbeddedCacheManager cacheContainer) {
      return SecurityActions.getCacheManagerConfiguration(cacheContainer);
   }

   private static void clearRunningTx(Cache<?, ?> cache) {
      if (cache != null) {
         TransactionManager txm = TestingUtil.getTransactionManager(cache);
         killTransaction(txm);
      }
   }

   public static void clearCacheLoader(Cache<?, ?> cache) {
      PersistenceManager persistenceManager = TestingUtil.extractComponent(cache, PersistenceManager.class);
      CompletionStages.join(persistenceManager.clearAllStores(BOTH));
   }

   public static <K, V> List<DummyInMemoryStore<K, V>> cachestores(List<Cache<K, V>> caches) {
      List<DummyInMemoryStore<K, V>> l = new LinkedList<>();
      for (Cache<K, V> c : caches)
         l.add(TestingUtil.getFirstStore(c));
      return l;
   }

   private static void removeInMemoryData(Cache<?, ?> cache) {
      log.debugf("Cleaning data for cache %s", cache);
      InternalDataContainer<?, ?> dataContainer = TestingUtil.extractComponent(cache, InternalDataContainer.class);
      if (log.isDebugEnabled())
         log.debugf("Data container size before clear: %d", dataContainer.sizeIncludingExpired());
      dataContainer.clear();
   }

   /**
    * Kills a cache - stops it, clears any data in any stores, and rolls back any associated txs
    */
   public static void killCaches(Cache<?, ?>... caches) {
      killCaches(Arrays.asList(caches));
   }

   /**
    * Kills a cache - stops it and rolls back any associated txs
    */
   public static void killCaches(Collection<? extends Cache<?, ?>> caches) {
      for (Cache<?, ?> c : caches) {
         try {
            if (c != null && c.getStatus() == ComponentStatus.RUNNING) {
               TransactionManager tm = c.getAdvancedCache().getTransactionManager();
               if (tm != null) {
                  try {
                     tm.rollback();
                  } catch (Exception e) {
                     // don't care
                  }
               }
               // retrieve the size before calling log, as evaluating the set may cause recursive log calls
               long size = log.isTraceEnabled() ? c.getAdvancedCache().withFlags(Flag.CACHE_MODE_LOCAL).size() : 0;
               if (c.getAdvancedCache().getRpcManager() != null) {
                  log.tracef("Local size on %s before stopping: %d", c.getAdvancedCache().getRpcManager().getAddress(), size);
               } else {
                  log.tracef("Local size before stopping: %d", size);
               }
               c.stop();
            }
         } catch (Throwable t) {
            log.errorf(t, "Error killing cache %s", c.getName());
         }
      }
   }

   /**
    * Clears transaction with the current thread in the given transaction manager.
    *
    * @param txManager a TransactionManager to be cleared
    */
   public static void killTransaction(TransactionManager txManager) {
      if (txManager != null) {
         try {
            if (txManager.getTransaction() != null) {
               txManager.rollback();
            }
         } catch (Exception e) {
            // don't care
         }
      }
   }


   /**
    * Clears any associated transactions with the current thread in the caches' transaction managers.
    */
   public static void killTransactions(Cache<?, ?>... caches) {
      for (Cache<?, ?> c : caches) {
         if (c != null && c.getStatus() == ComponentStatus.RUNNING) {
            TransactionManager tm = getTransactionManager(c);
            if (tm != null) {
               try {
                  tm.rollback();
               } catch (Exception e) {
                  // don't care
               }
            }
         }
      }
   }

   /**
    * For testing only - introspects a cache and extracts the ComponentRegistry
    *
    * @param cache cache to introspect
    * @return component registry
    */
   public static ComponentRegistry extractComponentRegistry(Cache<?, ?> cache) {
      return SecurityActions.getCacheComponentRegistry(cache.getAdvancedCache());
   }

   public static GlobalComponentRegistry extractGlobalComponentRegistry(CacheContainer cacheContainer) {
      return SecurityActions.getGlobalComponentRegistry((EmbeddedCacheManager) cacheContainer);
   }

   public static LockManager extractLockManager(Cache<?, ?> cache) {
      return extractComponentRegistry(cache).getComponent(LockManager.class);
   }

   public static Marshaller extractGlobalMarshaller(EmbeddedCacheManager cm) {
      GlobalComponentRegistry gcr = extractGlobalComponentRegistry(cm);
      return gcr.getComponent(Marshaller.class, KnownComponentNames.INTERNAL_MARSHALLER);
   }

   public static PersistenceMarshallerImpl extractPersistenceMarshaller(EmbeddedCacheManager cm) {
      return extractGlobalComponentRegistry(cm).getComponent(PersistenceMarshallerImpl.class, KnownComponentNames.PERSISTENCE_MARSHALLER);
   }

   public static AsyncInterceptorChain extractInterceptorChain(Cache<?, ?> cache) {
      return extractComponent(cache, AsyncInterceptorChain.class);
   }

   public static <K> LocalizedCacheTopology extractCacheTopology(Cache<K, ?> cache) {
      return cache.getAdvancedCache().getDistributionManager().getCacheTopology();
   }

   /**
    * Add a hook to cache startup sequence that will allow to replace existing component with a mock.
    */
   public static void addCacheStartingHook(EmbeddedCacheManager cacheContainer, BiConsumer<String, ComponentRegistry> consumer) {
      GlobalComponentRegistry gcr = extractGlobalComponentRegistry(cacheContainer);
      extractField(gcr, "moduleLifecycles");
      TestingUtil.<Collection<ModuleLifecycle>>replaceField(gcr, "moduleLifecycles", moduleLifecycles -> {
         Collection<ModuleLifecycle> copy = new ArrayList<>(moduleLifecycles);
         copy.add(new ModuleLifecycle() {
            @Override
            public void cacheStarting(ComponentRegistry cr, Configuration configuration, String cacheName) {
               consumer.accept(cacheName, cr);
            }
         });
         return copy;
      });
   }

   /**
    * Replaces an existing interceptor of the given type in the interceptor chain with a new interceptor
    * instance passed
    * as parameter.
    *
    * @param replacingInterceptor        the interceptor to add to the interceptor chain
    * @param toBeReplacedInterceptorType the type of interceptor that should be swapped with the new one
    * @return true if the interceptor was replaced
    */
   public static boolean replaceInterceptor(Cache<?, ?> cache, AsyncInterceptor replacingInterceptor, Class<? extends AsyncInterceptor> toBeReplacedInterceptorType) {
      ComponentRegistry cr = extractComponentRegistry(cache);
      // make sure all interceptors here are wired.
      cr.wireDependencies(replacingInterceptor);
      AsyncInterceptorChain inch = cr.getComponent(AsyncInterceptorChain.class);
      return inch.replaceInterceptor(replacingInterceptor, toBeReplacedInterceptorType);
   }

   /**
    * Blocks until the cache has reached a specified state.
    *
    * @param cache       cache to watch
    * @param cacheStatus status to wait for
    * @param timeout     timeout to wait for
    */
   public static void blockUntilCacheStatusAchieved(Cache<?, ?> cache, ComponentStatus cacheStatus, long timeout) {
      AdvancedCache<?, ?> spi = cache.getAdvancedCache();
      long killTime = System.currentTimeMillis() + timeout;
      while (System.currentTimeMillis() < killTime) {
         if (spi.getStatus() == cacheStatus) return;
         sleepThread(50);
      }
      throw new RuntimeException("Timed out waiting for condition");
   }

   public static void blockUntilViewsReceived(int timeout, Collection<?> caches) {
      Object first = caches.iterator().next();
      if (first instanceof Cache) {
         blockUntilViewsReceived(timeout, caches.toArray(new Cache[0]));
      } else {
         blockUntilViewsReceived(timeout, caches.toArray(new CacheContainer[0]));
      }
   }

   public static void blockUntilViewsReceived(int timeout, boolean barfIfTooManyMembers, Collection<?> caches) {
      Object first = caches.iterator().next();
      if (first instanceof Cache) {
         blockUntilViewsReceived(timeout, barfIfTooManyMembers, caches.toArray(new Cache[]{}));
      } else {
         blockUntilViewsReceived(timeout, barfIfTooManyMembers, caches.toArray(new CacheContainer[]{}));
      }
   }

   public static CommandsFactory extractCommandsFactory(Cache<?, ?> cache) {
      if (cache instanceof AbstractDelegatingCache) {
         // Need to unwrap to the base cache
         return extractCommandsFactory(extractField(cache, "cache"));
      }
      return (CommandsFactory) extractField(cache, "commandsFactory");
   }

   public static void dumpCacheContents(List<Cache<?, ?>> caches) {
      System.out.println("**** START: Cache Contents ****");
      int count = 1;
      for (Cache<?, ?> c : caches) {
         if (c == null) {
            System.out.println("  ** Cache " + count + " is null!");
         } else {
            EmbeddedCacheManager cacheManager = c.getCacheManager();
            System.out.println("  ** Cache " + count + " is " + cacheManager.getAddress());
         }
         count++;
      }
      System.out.println("**** END: Cache Contents ****");
   }

   public static void dumpCacheContents(Cache<?, ?>... caches) {
      dumpCacheContents(Arrays.asList(caches));
   }

   /**
    * Extracts a component of a given type from the cache's internal component registry
    */
   public static <T> T extractComponent(Cache<?, ?> cache, Class<T> componentType) {
      if (componentType == DataContainer.class) {
         throw new UnsupportedOperationException("Should extract InternalDataContainer");
      }
      ComponentRegistry cr = extractComponentRegistry(cache.getAdvancedCache());
      return cr.getComponent(componentType);
   }

   /**
    * Extracts a component of a given type from the cache's internal component registry
    */
   public static <T> T extractGlobalComponent(CacheContainer cacheContainer, Class<T> componentType) {
      GlobalComponentRegistry gcr = extractGlobalComponentRegistry(cacheContainer);
      return gcr.getComponent(componentType);
   }

   public static <T> T extractGlobalComponent(CacheContainer cacheContainer, Class<T> componentType, String componentName) {
      GlobalComponentRegistry gcr = extractGlobalComponentRegistry(cacheContainer);
      return gcr.getComponent(componentType, componentName);
   }

   public static TransactionManager getTransactionManager(Cache<?, ?> cache) {
      return cache == null ? null : cache.getAdvancedCache().getTransactionManager();
   }

   /**
    * Replaces a component in a running cache
    *
    * @param cache                cache in which to replace component
    * @param componentType        component type of which to replace
    * @param replacementComponent new instance
    * @param rewire               if true, ComponentRegistry.rewire() is called after replacing.
    * @return the original component that was replaced
    */
   public static <T> T replaceComponent(Cache<?, ?> cache, Class<? extends T> componentType, T replacementComponent, boolean rewire) {
      if (componentType == DataContainer.class) {
         throw new UnsupportedOperationException();
      }
      ComponentRegistry cr = extractComponentRegistry(cache);
      BasicComponentRegistry bcr = cr.getComponent(BasicComponentRegistry.class);
      ComponentRef<? extends T> old = bcr.getComponent(componentType);
      bcr.replaceComponent(componentType.getName(), replacementComponent, true);
      cr.cacheComponents();
      if (rewire) cr.rewire();
      return old != null ? old.wired() : null;
   }

   /**
    * Replaces a component in a running cache. This can also optionally stop the component before rewiring, which can be
    * important due to rewiring starts a component (you wouldn't want the component to be started twice).
    *
    * @param cache                cache in which to replace component
    * @param componentType        component type of which to replace
    * @param replacementComponent new instance
    * @param rewire               if true, ComponentRegistry.rewire() is called after replacing.
    * @param stopBeforeWire       stops the new component before wiring (the registry will start it again)
    * @return the original component that was replaced
    */
   public static <T extends Lifecycle> T replaceComponent(Cache<?, ?> cache, Class<T> componentType, T replacementComponent, boolean rewire,
                                                          boolean stopBeforeWire) {
      if (stopBeforeWire) {
         replacementComponent.stop();
      }
      return replaceComponent(cache, componentType, replacementComponent, rewire);
   }

   /**
    * Replaces a component in a running cache manager (global component registry)
    *
    * @param cacheContainer       cache in which to replace component
    * @param componentType        component type of which to replace
    * @param replacementComponent new instance
    * @param rewire               if true, ComponentRegistry.rewire() is called after replacing.
    * @return the original component that was replaced
    */
   public static <T> T replaceComponent(CacheContainer cacheContainer, Class<T> componentType, T replacementComponent,
                                        boolean rewire) {
      return replaceComponent(cacheContainer, componentType, componentType.getName(), replacementComponent, rewire);
   }

   /**
    * Same as {@link TestingUtil#replaceComponent(CacheContainer, Class, Object, boolean)} except that you can provide
    * an optional name, to replace specifically named components.
    *
    * @param cacheContainer       cache in which to replace component
    * @param componentType        component type of which to replace
    * @param name                 name of the component
    * @param replacementComponent new instance
    * @param rewire               if true, ComponentRegistry.rewire() is called after replacing.
    * @return the original component that was replaced
    */
   public static <T> T replaceComponent(CacheContainer cacheContainer, Class<T> componentType, String name, T replacementComponent, boolean rewire) {
      GlobalComponentRegistry cr = extractGlobalComponentRegistry(cacheContainer);
      BasicComponentRegistry bcr = cr.getComponent(BasicComponentRegistry.class);
      ComponentRef<T> old = bcr.getComponent(componentType);
      bcr.replaceComponent(name, replacementComponent, true);
      if (rewire) {
         cr.rewire();
         cr.rewireNamedRegistries();
      }
      return old != null ? old.wired() : null;
   }

   public static <K, V> CacheLoader<K, V> getCacheLoader(Cache<K, V> cache) {
      if (cache.getCacheConfiguration().persistence().usingStores()) {
         return TestingUtil.getFirstLoader(cache);
      } else {
         return null;
      }
   }

   public static String printCache(Cache<?, ?> cache) {
      DataContainer<?, ?> dataContainer = TestingUtil.extractComponent(cache, InternalDataContainer.class);
      Iterator<? extends InternalCacheEntry<?, ?>> it = dataContainer.iterator();
      StringBuilder builder = new StringBuilder(cache.getName() + "[");
      while (it.hasNext()) {
         InternalCacheEntry<?, ?> ce = it.next();
         builder.append(ce.getKey()).append("=").append(ce.getValue()).append(",l=").append(ce.getLifespan())
               .append("; ");
      }
      builder.append("]");
      return builder.toString();
   }

   public static <K> Set<K> getInternalKeys(Cache<K, ?> cache) {
      DataContainer<K, ?> dataContainer = cache.getAdvancedCache().getDataContainer();
      Set<K> keys = new HashSet<>();
      for (CacheEntry<K, ?> entry : dataContainer) {
         keys.add(entry.getKey());
      }
      return keys;
   }

   public static <V> Collection<V> getInternalValues(Cache<?, V> cache) {
      DataContainer<?, V> dataContainer = cache.getAdvancedCache().getDataContainer();
      Collection<V> values = new ArrayList<>();
      for (CacheEntry<?, V> entry : dataContainer) {
         values.add(entry.getValue());
      }
      return values;
   }

   public static DISCARD getDiscardForCache(EmbeddedCacheManager cacheManager) throws Exception {
      JGroupsTransport jgt = (JGroupsTransport) TestingUtil.extractGlobalComponent(cacheManager, Transport.class);
      JChannel ch = jgt.getChannel();
      ProtocolStack ps = ch.getProtocolStack();
      DISCARD discard = new DISCARD();
      discard.excludeItself(false);
      ps.insertProtocol(discard, ProtocolStack.Position.ABOVE, TP.class);
      return discard;
   }

   /**
    * Inserts a DELAY protocol in the JGroups stack used by the cache, and returns it.
    * The DELAY protocol can then be used to inject delays in milliseconds both at receiver
    * and sending side.
    *
    * @param cache            cache to inject
    * @param in_delay_millis  inbound delay in millis
    * @param out_delay_millis outbound delay in millis
    * @return a reference to the DELAY instance being used by the JGroups stack
    */
   public static DELAY setDelayForCache(Cache<?, ?> cache, int in_delay_millis, int out_delay_millis) throws Exception {
      JGroupsTransport jgt = (JGroupsTransport) TestingUtil.extractComponent(cache, Transport.class);
      JChannel ch = jgt.getChannel();
      ProtocolStack ps = ch.getProtocolStack();
      DELAY delay = ps.findProtocol(DELAY.class);
      if (delay == null) {
         delay = new DELAY();
         ps.insertProtocol(delay, ProtocolStack.Position.ABOVE, TP.class);
      }
      delay.setInDelay(in_delay_millis);
      delay.setOutDelay(out_delay_millis);
      return delay;
   }

   public static String k() {
      return k(0);
   }

   public static String k(int index) {
      return k(CallerId.getCallerMethodName(2), index);
   }

   public static String k(int index, String prefix) {
      return String.format("%s-k%d-%s", prefix, index, CallerId.getCallerMethodName(2));
   }

   public static String v() {
      return v(0);
   }

   public static String v(int index) {
      return v(CallerId.getCallerMethodName(2), index);
   }

   public static String v(int index, String prefix) {
      return String.format("%s-v%d-%s", prefix, index, CallerId.getCallerMethodName(2));
   }

   public static String k(Method method, int index) {
      return "k" + index + '-' + method.getName();
   }

   public static String k(String method, int index) {
      return "k" + index + '-' + method;
   }

   public static String v(Method method, int index) {
      return "v" + index + '-' + method.getName();
   }

   public static String v(String method, int index) {
      return "v" + index + '-' + method;
   }

   public static String k(Method method) {
      return k(method, 0);
   }

   public static String v(Method method) {
      return v(method, 0);
   }

   public static String k(Method m, String prefix) {
      return prefix + m.getName();
   }

   public static String v(Method m, String prefix) {
      return prefix + m.getName();
   }

   public static String v(Method m, String prefix, int index) {
      return String.format("%s-v%d-%s", prefix, index, m.getName());
   }

   public static TransactionTable getTransactionTable(Cache<?, ?> cache) {
      return extractComponent(cache, TransactionTable.class);
   }

   public static ObjectName getCacheManagerObjectName(String jmxDomain) {
      return getCacheManagerObjectName(jmxDomain, "DefaultCacheManager");
   }

   public static ObjectName getCacheManagerObjectName(String jmxDomain, String cacheManagerName) {
      return getCacheManagerObjectName(jmxDomain, cacheManagerName, "CacheManager");
   }

   public static ObjectName getCacheManagerObjectName(String jmxDomain, String cacheManagerName, String component) {
      try {
         return new ObjectName(jmxDomain + ":type=CacheManager,name=" + ObjectName.quote(cacheManagerName) + ",component=" + component);
      } catch (MalformedObjectNameException e) {
         throw new RuntimeException(e);
      }
   }

   public static ObjectName getCacheObjectName(String jmxDomain, String cacheName) {
      return getCacheObjectName(jmxDomain, cacheName, "Cache");
   }

   public static ObjectName getCacheObjectName(String jmxDomain, String cacheName, String component) {
      return getCacheObjectName(jmxDomain, cacheName, component, "DefaultCacheManager");
   }

   public static ObjectName getCacheObjectName(String jmxDomain, String cacheName, String component, String cacheManagerName) {
      if (!cacheName.contains("(") || !cacheName.endsWith(")")) {
         throw new IllegalArgumentException("Cache name does not appear to include a cache mode suffix: " + cacheName);
      }
      try {
         return new ObjectName(jmxDomain + ":type=Cache,manager=" + ObjectName.quote(cacheManagerName)
               + ",name=" + ObjectName.quote(cacheName) + ",component=" + component);
      } catch (MalformedObjectNameException e) {
         throw new RuntimeException(e);
      }
   }

   public static ObjectName getJGroupsChannelObjectName(EmbeddedCacheManager cacheManager) {
      GlobalConfiguration cfg = cacheManager.getCacheManagerConfiguration();
      try {
         return new ObjectName(String.format("%s:type=channel,cluster=%s,manager=%s",
               cfg.jmx().domain(),
               ObjectName.quote(cacheManager.getClusterName()),
               ObjectName.quote(cfg.cacheManagerName())));
      } catch (MalformedObjectNameException e) {
         throw new RuntimeException(e);
      }
   }

   public static boolean existsDomain(MBeanServer mBeanServer, String domain) {
      for (String d : mBeanServer.getDomains()) {
         if (domain.equals(d)) return true;
      }
      return false;
   }

   public static void checkMBeanOperationParameterNaming(MBeanServer mBeanServer, ObjectName objectName) throws JMException {
      MBeanInfo mBeanInfo = mBeanServer.getMBeanInfo(objectName);
      for (MBeanOperationInfo op : mBeanInfo.getOperations()) {
         for (MBeanParameterInfo param : op.getSignature()) {
            // assert that all operation parameters have a proper name (not an autogenerated p0, p1, ...
            assertFalse(param.getName().matches("p[0-9]+"));
         }
      }
   }

   public static String generateRandomString(int numberOfChars) {
      return generateRandomString(numberOfChars, new Random(System.currentTimeMillis()));
   }

   public static String generateRandomString(int numberOfChars, Random r) {
      StringBuilder sb = new StringBuilder();
      for (int i = 0; i < numberOfChars; i++) sb.append((char) (64 + r.nextInt(26)));
      return sb.toString();
   }

   /**
    * Verifies the cache doesn't contain any lock
    */
   public static void assertNoLocks(Cache<?, ?> cache) {
      LockManager lm = TestingUtil.extractLockManager(cache);
      if (lm != null) {
         for (Object key : cache.keySet()) assert !lm.isLocked(key);
      }
   }

   /**
    * Call an operation within a transaction. This method guarantees that the
    * right pattern is used to make sure that the transaction is always either
    * committed or rollbacked.
    *
    * @param tm  transaction manager
    * @param c   callable instance to run within a transaction
    * @param <T> type returned from the callable
    * @return returns whatever the callable returns
    */
   public static <T> T withTx(TransactionManager tm, Callable<T> c) throws Exception {
      return ((Callable<T>) () -> {
         tm.begin();
         try {
            return ((Callable<? extends T>) c).call();
         } catch (Exception e) {
            tm.setRollbackOnly();
            throw e;
         } finally {
            if (tm.getStatus() == Status.STATUS_ACTIVE) tm.commit();
            else tm.rollback();
         }
      }).call();
   }

   /**
    * Invoke a task using a cache manager. This method guarantees that the
    * cache manager used in the task will be cleaned up after the task has
    * completed, regardless of the task outcome.
    *
    * @param c task to execute
    */
   public static void withCacheManager(CacheManagerCallable c) {
      try {
         c.call();
      } catch (RuntimeException e) {
         throw e;
      } catch (Exception e) {
         throw new RuntimeException(e);
      } finally {
         if (c.clearBeforeKill()) {
            TestingUtil.clearContent(c.cm);
         }
         TestingUtil.killCacheManagers(c.cm);
      }
   }

   /**
    * Invoke a task using a cache manager created by given supplier function.
    * This method guarantees that the cache manager created in the task will
    * be cleaned up after the task has completed, regardless of the task outcome.
    *
    * @param s cache manager supplier function
    * @param c consumer function to execute with cache manager
    */
   public static void withCacheManager(Supplier<EmbeddedCacheManager> s,
                                       Consumer<EmbeddedCacheManager> c) {
      EmbeddedCacheManager cm = null;
      try {
         cm = s.get();
         c.accept(cm);
      } finally {
         if (cm != null) TestingUtil.killCacheManagers(cm);
      }
   }

   /**
    * Invoke a task using a cache manager.
    * This method guarantees that the cache manager will
    * be cleaned up after the task has completed, regardless of the task outcome.
    *
    * @param cm cache manager
    * @param c  consumer function to execute with cache manager
    */
   public static void withCacheManager(EmbeddedCacheManager cm,
                                       Consumer<EmbeddedCacheManager> c) {
      try {
         c.accept(cm);
      } finally {
         TestingUtil.killCacheManagers(cm);
      }
   }

   /**
    * Invoke a task using a several cache managers. This method guarantees
    * that the cache managers used in the task will be cleaned up after the
    * task has completed, regardless of the task outcome.
    *
    * @param c task to execute
    */
   public static void withCacheManagers(MultiCacheManagerCallable c) {
      try {
         c.call();
      } catch (RuntimeException e) {
         throw e;
      } catch (Exception e) {
         throw new RuntimeException(e);
      } finally {
         TestingUtil.killCacheManagers(c.cms);
      }
   }

   public static String getDefaultCacheName(EmbeddedCacheManager cm) {
      return extractGlobalConfiguration(cm).defaultCacheName().get();
   }


   /**
    * Returns true if at least "duration" millis elapsed since the specified "start" time (millis).
    */
   public static boolean moreThanDurationElapsed(long start, long duration) {
      return now() - duration >= start;
   }

   /**
    * Returns current CPU time in millis.
    */
   public static long now() {
      return TimeUnit.NANOSECONDS.toMillis(System.nanoTime());
   }

   public static Metadata metadata(Long lifespan, Long maxIdle) {
      return new EmbeddedMetadata.Builder().lifespan(lifespan != null ? lifespan : -1)
            .maxIdle(maxIdle != null ? maxIdle : -1).build();
   }

   public static Metadata metadata(Integer lifespan, Integer maxIdle) {
      return new EmbeddedMetadata.Builder().lifespan(lifespan != null ? lifespan : -1)
            .maxIdle(maxIdle != null ? maxIdle : -1).build();
   }

   public static <T extends NonBlockingStore<K, V>, K, V> T getFirstStore(Cache<K, V> cache) {
      return getStore(cache, 0, true);
   }

   @SuppressWarnings({"unchecked", "unchecked cast"})
   public static <T extends NonBlockingStore<K, V>, K, V> T getStore(Cache<K, V> cache, int position, boolean unwrapped) {
      PersistenceManagerImpl persistenceManager = getActualPersistenceManager(cache);
      NonBlockingStore<K, V> nonBlockingStore = persistenceManager.<K, V>getAllStores(characteristics ->
            !characteristics.contains(NonBlockingStore.Characteristic.WRITE_ONLY)).get(position);
      if (unwrapped && nonBlockingStore instanceof DelegatingNonBlockingStore) {
         nonBlockingStore = ((DelegatingNonBlockingStore<K, V>) nonBlockingStore).delegate();
      }
      return (T) nonBlockingStore;
   }

   public static <K, V> WaitDelegatingNonBlockingStore<K, V> getFirstStoreWait(Cache<K, V> cache) {
      return getStoreWait(cache, 0, true);
   }

   @SuppressWarnings({"cast"})
   public static <K, V> WaitDelegatingNonBlockingStore<K, V> getStoreWait(Cache<K, V> cache, int position, boolean unwrapped) {
      NonBlockingStore<K, V> nonBlockingStore = getStore(cache, position, unwrapped);
      KeyPartitioner keyPartitioner = extractComponent(cache, KeyPartitioner.class);
      return new WaitDelegatingNonBlockingStore<>(nonBlockingStore, keyPartitioner);
   }

   public static <T extends CacheLoader<K, V>, K, V> T getFirstLoader(Cache<K, V> cache) {
      PersistenceManagerImpl persistenceManager = getActualPersistenceManager(cache);
      NonBlockingStore<K, V> nonBlockingStore = persistenceManager.<K, V>getAllStores(characteristics ->
            !characteristics.contains(NonBlockingStore.Characteristic.WRITE_ONLY)).get(0);
      // TODO: Once stores convert to non blocking implementations this will change
      //noinspection unchecked
      return (T) ((NonBlockingStoreAdapter<K, V>) nonBlockingStore).loader();
   }

   @SuppressWarnings("unchecked")
   public static <T extends CacheWriter<K, V>, K, V> T getFirstWriter(Cache<K, V> cache) {
      return getWriter(cache, 0);
   }

   public static <T extends CacheWriter<K, V>, K, V> T getWriter(Cache<K, V> cache, int position) {
      PersistenceManagerImpl persistenceManager = getActualPersistenceManager(cache);
      NonBlockingStore<K, V> nonBlockingStore = persistenceManager.<K, V>getAllStores(characteristics ->
            !characteristics.contains(NonBlockingStore.Characteristic.READ_ONLY)).get(position);
      // TODO: Once stores convert to non blocking implementations this will change
      return (T) ((NonBlockingStoreAdapter<K, V>) nonBlockingStore).writer();
   }

   @SuppressWarnings("unchecked")
   public static <T extends CacheWriter<K, V>, K, V> T getFirstTxWriter(Cache<K, V> cache) {
      PersistenceManagerImpl persistenceManager = getActualPersistenceManager(cache);
      NonBlockingStore<K, V> nonBlockingStore = persistenceManager.<K, V>getAllStores(characteristics ->
            characteristics.contains(NonBlockingStore.Characteristic.TRANSACTIONAL)).get(0);
      // TODO: Once stores convert to non blocking implementations this will change
      return (T) ((NonBlockingStoreAdapter<K, V>) nonBlockingStore).transactionalStore();
   }

   private static PersistenceManagerImpl getActualPersistenceManager(Cache<?, ?> cache) {
      PersistenceManager persistenceManager = extractComponent(cache, PersistenceManager.class);
      if (persistenceManager instanceof DelegatingPersistenceManager) {
         return (PersistenceManagerImpl) ((DelegatingPersistenceManager) persistenceManager).getActual();
      }
      return (PersistenceManagerImpl) persistenceManager;
   }

   public static <K, V> Set<MarshallableEntry<K, V>> allEntries(AdvancedLoadWriteStore<K, V> cl, Predicate<K> filter) {
      return Flowable.fromPublisher(cl.entryPublisher(filter, true, true))
            .collectInto(new HashSet<MarshallableEntry<K, V>>(), Set::add)
            .blockingGet();
   }

   public static <K, V> Set<MarshallableEntry<K, V>> allEntries(AdvancedLoadWriteStore<K, V> cl) {
      return allEntries(cl, null);
   }

   public static <K, V> Set<MarshallableEntry<K, V>> allEntries(NonBlockingStore<K, V> store) {
      return allEntries(store, IntSets.immutableSet(0), null);
   }

   public static <K, V> Set<MarshallableEntry<K, V>> allEntries(NonBlockingStore<K, V> store, Predicate<? super K> filter) {
      return allEntries(store, IntSets.immutableSet(0), filter);
   }

   public static <K, V> Set<MarshallableEntry<K, V>> allEntries(NonBlockingStore<K, V> store, IntSet segments) {
      return allEntries(store, segments, null);
   }

   public static <K, V> Set<MarshallableEntry<K, V>> allEntries(NonBlockingStore<K, V> store, IntSet segments,
                                                                Predicate<? super K> filter) {
      return Flowable.fromPublisher(store.publishEntries(segments, filter, true))
            .collectInto(new HashSet<MarshallableEntry<K, V>>(), Set::add)
            .blockingGet();
   }

   public static void outputPropertiesToXML(String outputFile, Properties properties) throws IOException {
      Properties sorted = new Properties() {
         @Override
         public Set<Object> keySet() {
            return Collections.unmodifiableSet(new TreeSet<>(super.keySet()));
         }

         @Override
         public synchronized Enumeration<Object> keys() {
            return Collections.enumeration(new TreeSet<>(super.keySet()));
         }

         @Override
         public Set<String> stringPropertyNames() {
            return Collections.unmodifiableSet(new TreeSet<>(super.stringPropertyNames()));
         }
      };
      sorted.putAll(properties);
      try (OutputStream stream = new FileOutputStream(outputFile)) {
         sorted.storeToXML(stream, null);
      }
   }

   public static <K, V> void writeToAllStores(K key, V value, Cache<K, V> cache) {
      PersistenceManager pm = extractComponent(cache, PersistenceManager.class);
      KeyPartitioner keyPartitioner = extractComponent(cache, KeyPartitioner.class);
      CompletionStages.join(pm.writeToAllNonTxStores(MarshalledEntryUtil.create(key, value, cache), keyPartitioner.getSegment(key), BOTH));
   }

   public static <K, V> boolean deleteFromAllStores(K key, Cache<K, V> cache) {
      PersistenceManager pm = extractComponent(cache, PersistenceManager.class);
      KeyPartitioner keyPartitioner = extractComponent(cache, KeyPartitioner.class);
      return CompletionStages.join(pm.deleteFromAllStores(key, keyPartitioner.getSegment(key), BOTH));
   }

   public static Subject makeSubject(String... principals) {
      Set<Principal> set = new LinkedHashSet<>();
      if (principals.length > 0) {
         set.add(new TestingUtil.TestPrincipal(principals[0]));
         for (int i = 1; i < principals.length; i++) {
            set.add(new GroupPrincipal(principals[i]));
         }
      }
      return new Subject(true, set, Collections.emptySet(), Collections.emptySet());
   }

   public static Map<AuthorizationPermission, Subject> makeAllSubjects() {
      HashMap<AuthorizationPermission, Subject> subjects = new HashMap<>(AuthorizationPermission.values().length);
      for (AuthorizationPermission perm : AuthorizationPermission.values()) {
         subjects.put(perm, makeSubject(perm.toString() + "_user", perm.toString()));
      }
      return subjects;
   }

   static public void assertAnyEquals(Object expected, Object actual) {
      if (expected instanceof byte[] && actual instanceof byte[])
         AssertJUnit.assertArrayEquals((byte[]) expected, (byte[]) actual);
      else
         AssertJUnit.assertEquals(expected, actual);
   }

   public static void assertBetween(double lowerBound, double upperBound, double actual) {
      if (actual < lowerBound || upperBound < actual) {
         fail("Expected between:<" + lowerBound + "> and:<" + upperBound + "> but was:<" + actual + ">");
      }
   }

   public static void assertBetween(long lowerBound, long upperBound, long actual) {
      if (actual < lowerBound || upperBound < actual) {
         fail("Expected between:<" + lowerBound + "> and:<" + upperBound + "> but was:<" + actual + ">");
      }
   }

   public static MBeanServer getMBeanServer(Cache<?, ?> cache) {
      return extractComponent(cache, CacheJmxRegistration.class).getMBeanServer();
   }

   public static MBeanServer getMBeanServer(EmbeddedCacheManager cacheManager) {
      return extractGlobalComponent(cacheManager, CacheManagerJmxRegistration.class).getMBeanServer();
   }

   public static class TestPrincipal implements Principal, Serializable {
      String name;

      public TestPrincipal(String name) {
         this.name = name;
      }

      @Override
      public String getName() {
         return name;
      }

      @Override
      public String toString() {
         return "TestPrincipal [name=" + name + "]";
      }

      @Override
      public int hashCode() {
         final int prime = 31;
         int result = 1;
         result = prime * result + ((name == null) ? 0 : name.hashCode());
         return result;
      }

      @Override
      public boolean equals(Object obj) {
         if (this == obj)
            return true;
         if (obj == null)
            return false;
         if (getClass() != obj.getClass())
            return false;
         TestPrincipal other = (TestPrincipal) obj;
         if (name == null) {
            return other.name == null;
         } else return name.equals(other.name);
      }
   }

   public static <T, W extends T> W wrapGlobalComponent(CacheContainer cacheContainer, Class<T> tClass,
                                                        WrapFactory<T, W, CacheContainer> factory, boolean rewire) {
      T current = extractGlobalComponent(cacheContainer, tClass);
      W wrap = factory.wrap(cacheContainer, current);
      replaceComponent(cacheContainer, tClass, wrap, rewire);
      return wrap;
   }

   public static <T, W extends T> W wrapGlobalComponent(CacheContainer cacheContainer, Class<T> tClass,
                                                        Function<T, W> ctor, boolean rewire) {
      T current = extractGlobalComponent(cacheContainer, tClass);
      W wrap = ctor.apply(current);
      replaceComponent(cacheContainer, tClass, wrap, rewire);
      return wrap;
   }

   public static <T, W extends T> W wrapComponent(Cache<?, ?> cache, Class<T> tClass,
                                                  WrapFactory<T, W, Cache<?, ?>> factory, boolean rewire) {
      T current = extractComponent(cache, tClass);
      W wrap = factory.wrap(cache, current);
      replaceComponent(cache, tClass, wrap, rewire);
      return wrap;
   }

   public static <T, W extends T> W wrapComponent(Cache<?, ?> cache, Class<T> tClass, Function<T, W> ctor) {
      T current = extractComponent(cache, tClass);
      W wrap = ctor.apply(current);
      replaceComponent(cache, tClass, wrap, true);
      return wrap;
   }

   public static <T extends PerCacheInboundInvocationHandler>
   T wrapInboundInvocationHandler(Cache<?, ?> cache, Function<PerCacheInboundInvocationHandler, T> ctor) {
      PerCacheInboundInvocationHandler current = extractComponent(cache, PerCacheInboundInvocationHandler.class);
      T wrap = ctor.apply(current);
      replaceComponent(cache, PerCacheInboundInvocationHandler.class, wrap, true);
      return wrap;
   }

   public interface WrapFactory<T, W, C> {
      W wrap(C wrapOn, T current);
   }

   public static void expectCause(Throwable t, Class<? extends Throwable> c, String messageRegex) {
      for (; ; ) {
         if (c.isAssignableFrom(t.getClass())) {
            if (messageRegex != null && !Pattern.matches(messageRegex, t.getMessage())) {
               throw new RuntimeException(String.format("Exception message '%s' does not match regex '%s'", t.getMessage(), messageRegex), t);
            }
            return;
         }
         Throwable cause = t.getCause();
         if (cause == null || cause == t) {
            throw new RuntimeException("Cannot find a cause of type " + c.getName(), cause);
         } else {
            t = cause;
         }
      }
   }

   public static boolean isTriangleAlgorithm(CacheMode cacheMode, boolean transactional) {
      return cacheMode.isDistributed() && !transactional;
   }

   public static <K, V> Map.Entry<K, V> createMapEntry(K key, V value) {
      return new AbstractMap.SimpleEntry<>(key, value);
   }

   public static <T, U> Map<T, U> mapOf(Object... keyValueKeyValueKeyValue) {
      Map<T, U> map = new HashMap<>();
      for (int i = 0; i < keyValueKeyValueKeyValue.length; ) {
         map.put((T) keyValueKeyValueKeyValue[i++], (U) keyValueKeyValueKeyValue[i++]);
      }
      return map;
   }

   @SafeVarargs
   public static <T> Set<T> setOf(T... elements) {
      if (elements == null)
         return Collections.emptySet();

      return new HashSet<>(Arrays.asList(elements));
   }

   /**
    * This method sets only fields annotated with <code>@Inject</code>, it does not invoke any injecting methods.
    * Named setters are not handled either.
    */
   public static void inject(Object instance, Object... components) {
      TestComponentAccessors.wire(instance, components);
   }

   public static void startComponent(Object component) {
      try {
         TestComponentAccessors.start(component);
      } catch (Exception e) {
         throw new TestException(e);
      }
   }

   public static void stopComponent(Object component) {
      try {
         TestComponentAccessors.stop(component);
      } catch (Exception e) {
         throw new TestException(e);
      }
   }

   public static Object named(String name, Object instance) {
      return new TestComponentAccessors.NamedComponent(name, instance);
   }

   public static void cleanUpDataContainerForCache(Cache<?, ?> cache) {
      InternalDataContainer<?, ?> dataContainer = extractComponent(cache, InternalDataContainer.class);
      dataContainer.cleanUp();
   }

   public static void copy(InputStream is, OutputStream os) throws IOException {
      byte[] buffer = new byte[1024];
      int length;
      while ((length = is.read(buffer)) > 0) {
         os.write(buffer, 0, length);
      }
   }

   public static ProtoStreamMarshaller createProtoStreamMarshaller(SerializationContextInitializer sci) {
      SerializationContext ctx = ProtobufUtil.newSerializationContext();
      sci.registerSchema(ctx);
      sci.registerMarshallers(ctx);
      return new ProtoStreamMarshaller(ctx);
   }

   public static <E> Publisher<NonBlockingStore.SegmentedPublisher<E>> singleSegmentPublisher(Publisher<E> flowable) {
      return Flowable.just(SingleSegmentPublisher.singleSegment(flowable));
   }

   public static <E> Publisher<NonBlockingStore.SegmentedPublisher<E>> multipleSegmentPublisher(Publisher<E> flowable,
                                                                                                Function<E, Object> toKeyFunction, KeyPartitioner keyPartitioner) {
      return Flowable.fromPublisher(flowable)
            .groupBy(e -> keyPartitioner.getSegment(toKeyFunction.apply(e)))
            .map(SegmentPublisherWrapper::wrap);
   }

   public static void defineConfiguration(EmbeddedCacheManager cacheManager, String cacheName, Configuration configuration) {
      SecurityActions.defineConfiguration(cacheManager, cacheName, configuration);
   }

   public static Set<Object> getListeners(Cache<?, ?> cache) {
      CacheNotifierImpl<?, ?> notifier = (CacheNotifierImpl<?, ?>) extractComponent(cache, CacheNotifier.class);
      return notifier.getListeners();
   }

   public static Set<Object> getListeners(EmbeddedCacheManager cacheManager) {
      CacheManagerNotifierImpl notifier = (CacheManagerNotifierImpl) extractGlobalComponent(cacheManager, CacheManagerNotifier.class);
      return notifier.getListeners();
   }
}
