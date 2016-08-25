package org.infinispan.test;

import static java.io.File.separator;
import static org.infinispan.commons.api.BasicCacheContainer.DEFAULT_CACHE_NAME;
import static org.infinispan.persistence.manager.PersistenceManager.AccessMode.BOTH;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.fail;

import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.security.Principal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import javax.management.MBeanInfo;
import javax.management.MBeanOperationInfo;
import javax.management.MBeanParameterInfo;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.security.auth.Subject;
import javax.transaction.Status;
import javax.transaction.TransactionManager;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.cache.impl.CacheImpl;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.InternalCacheValue;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.InvocationContextFactory;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.filter.KeyFilter;
import org.infinispan.interceptors.AsyncInterceptor;
import org.infinispan.interceptors.AsyncInterceptorChain;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.jmx.PerThreadMBeanServerLookup;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.manager.CacheContainer;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.marshall.core.ExternalizerTable;
import org.infinispan.marshall.core.MarshalledEntry;
import org.infinispan.marshall.core.MarshalledEntryImpl;
import org.infinispan.metadata.EmbeddedMetadata;
import org.infinispan.metadata.Metadata;
import org.infinispan.metadata.impl.InternalMetadataImpl;
import org.infinispan.persistence.PersistenceUtil;
import org.infinispan.persistence.manager.PersistenceManager;
import org.infinispan.persistence.manager.PersistenceManagerImpl;
import org.infinispan.persistence.spi.AdvancedCacheLoader;
import org.infinispan.persistence.spi.AdvancedLoadWriteStore;
import org.infinispan.persistence.spi.CacheLoader;
import org.infinispan.persistence.spi.CacheWriter;
import org.infinispan.remoting.inboundhandler.PerCacheInboundInvocationHandler;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.remoting.transport.jgroups.JGroupsTransport;
import org.infinispan.security.impl.SecureCacheImpl;
import org.infinispan.statetransfer.StateTransferManager;
import org.infinispan.topology.CacheTopology;
import org.infinispan.transaction.impl.TransactionTable;
import org.infinispan.util.DependencyGraph;
import org.infinispan.util.concurrent.TimeoutException;
import org.infinispan.util.concurrent.WithinThreadExecutor;
import org.infinispan.util.concurrent.locks.LockManager;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.jgroups.JChannel;
import org.jgroups.View;
import org.jgroups.protocols.DELAY;
import org.jgroups.protocols.DISCARD;
import org.jgroups.protocols.TP;
import org.jgroups.stack.ProtocolStack;
import org.testng.AssertJUnit;

public class TestingUtil {
   private static final Log log = LogFactory.getLog(TestingUtil.class);
   private static final Random random = new Random();
   public static final String TEST_PATH = "infinispanTempFiles";
   public static final String JGROUPS_CONFIG = "<jgroups>\n" +
         "      <stack-file name=\"tcp\" path=\"jgroups-tcp.xml\"/>\n" +
         "   </jgroups>";

   public enum InfinispanStartTag {
      START_40(4, 0),
      START_41(4, 1),
      START_42(4, 2),
      START_50(5, 0),
      START_51(5, 1),
      START_52(5, 2),
      START_53(5, 3),
      START_60(6, 0),
      START_70(7, 0),
      START_71(7, 1),
      START_72(7, 2),
      START_80(8, 0);

      public static final InfinispanStartTag LATEST = START_80;
      private final String tag;
      private final String majorMinor;

      InfinispanStartTag(int major, int minor) {
         tag = String.format("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<infinispan\n" +
         "      xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"\n" +
         "      xsi:schemaLocation=\"urn:infinispan:config:%d.%d http://www.infinispan.org/schemas/infinispan-config-%d.%d.xsd\"\n" +
         "      xmlns=\"urn:infinispan:config:%d.%d\">", major, minor, major, minor, major, minor);
         majorMinor = String.format("%d.%d", major, minor);
      }

      @Override
      public String toString() {
         return tag;
      }

      public String majorMinor() {
         return majorMinor;
      }
   }

   public static final String INFINISPAN_END_TAG = "</infinispan>";
   public static final String INFINISPAN_START_TAG_NO_SCHEMA = "<infinispan>";


   /**
    * Extracts the value of a field in a given target instance using reflection, able to extract private fields as
    * well.
    *
    * @param target    object to extract field from
    * @param fieldName name of field to extract
    *
    * @return field value
    */
   public static <T> T extractField(Object target, String fieldName) {
      return (T) extractField(target.getClass(), target, fieldName);
   }

   public static void replaceField(Object newValue, String fieldName, Object owner, Class baseType) {
      Field field;
      try {
         field = baseType.getDeclaredField(fieldName);
         field.setAccessible(true);
         field.set(owner, newValue);
      }
      catch (Exception e) {
         throw new RuntimeException(e);//just to simplify exception handling
      }
   }


   public static Object extractField(Class type, Object target, String fieldName) {
      while (true) {
         Field field;
         try {
            field = type.getDeclaredField(fieldName);
            field.setAccessible(true);
            return field.get(target);
         }
         catch (Exception e) {
            if (type.equals(Object.class)) {
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
      return cache.getAdvancedCache().getAsyncInterceptorChain()
            .findInterceptorExtending(interceptorToFind);
   }

   public static void waitForRehashToComplete(Cache... caches) {
      final int REHASH_TIMEOUT_SECONDS = 60; //Needs to be rather large to prevent sporadic failures on CI
      final long giveup = System.nanoTime() + TimeUnit.SECONDS.toNanos(REHASH_TIMEOUT_SECONDS);
      for (Cache c : caches) {
         if (c instanceof SecureCacheImpl) {
            c = (Cache) extractField(SecureCacheImpl.class, c, "delegate");
         }
         StateTransferManager stateTransferManager = extractComponent(c, StateTransferManager.class);
         Address cacheAddress = c.getAdvancedCache().getRpcManager().getAddress();
         while (true) {
            CacheTopology cacheTopology = stateTransferManager.getCacheTopology();
            boolean rebalanceInProgress;
            boolean chContainsAllMembers;
            boolean currentChIsBalanced;
            if (cacheTopology != null) {
               rebalanceInProgress = cacheTopology.getPendingCH() != null;
               ConsistentHash currentCH = cacheTopology.getCurrentCH();

               chContainsAllMembers = currentCH.getMembers().size() == caches.length;
               currentChIsBalanced = true;

               int actualNumOwners = Math.min(currentCH.getNumOwners(), currentCH.getMembers().size());
               for (int i = 0; i < currentCH.getNumSegments(); i++) {
                  if (currentCH.locateOwnersForSegment(i).size() < actualNumOwners) {
                     currentChIsBalanced = false;
                     break;
                  }
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
                  message = String.format("Timed out waiting for rebalancing to complete on node %s, " +
                        "expected member list is %s, current member list is %s!",
                        cacheAddress, Arrays.toString(addresses), cacheTopology.getCurrentCH().getMembers());
               } else {
                  message = String.format("Timed out waiting for rebalancing to complete on node %s, " +
                        "current topology is %s. rebalanceInProgress=%s, currentChIsBalanced=%s", c.getCacheManager().getAddress(),
                                          cacheTopology, rebalanceInProgress, currentChIsBalanced);
               }
               log.error(message);
               throw new RuntimeException(message);
            }

            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(100));
         }
         log.trace("Node " + cacheAddress + " finished state transfer.");
      }
   }

   public static void waitForRehashToComplete(Collection<? extends Cache> caches) {
      waitForRehashToComplete(caches.toArray(new Cache[caches.size()]));
   }

   /**
    * Loops, continually calling {@link #areCacheViewsComplete(Cache[])} until it either returns true or
    * <code>timeout</code> ms have elapsed.
    *
    * @param caches  caches which must all have consistent views
    * @param timeout max number of ms to loop
    *
    * @throws RuntimeException if <code>timeout</code> ms have elapse without all caches having the same number of
    *                          members.
    */
   public static void blockUntilViewsReceived(Cache[] caches, long timeout) {
      long failTime = System.currentTimeMillis() + timeout;

      while (System.currentTimeMillis() < failTime) {
         sleepThread(100);
         if (areCacheViewsComplete(caches)) {
            return;
         }
      }

      viewsTimedOut(caches);
   }

   private static void viewsTimedOut(Cache[] caches) {
      CacheContainer[] cacheContainers = new CacheContainer[caches.length];
      for (int i = 0; i < caches.length; i++) {
         cacheContainers[i] = caches[i].getCacheManager();
      }
      viewsTimedOut(cacheContainers);
   }
   private static void viewsTimedOut(CacheContainer[] cacheContainers) {
      int length = cacheContainers.length;
      List<View> incompleteViews = new ArrayList<>(length);
      for (int i = 0; i < length; i++) {
         EmbeddedCacheManager cm = (EmbeddedCacheManager) cacheContainers[i];
         if (cm.getMembers().size() != cacheContainers.length) {
            incompleteViews.add(((JGroupsTransport) cm.getTransport()).getChannel().getView());
            log.warnf("Manager %s has an incomplete view: %s", cm.getAddress(), cm.getMembers());
         }
      }

      throw new TimeoutException(String.format(
         "Timed out before caches had complete views.  Expected %d members in each view.  Views are as follows: %s",
         cacheContainers.length, incompleteViews));
   }

   public static void blockUntilViewsReceivedInt(Cache[] caches, long timeout) throws InterruptedException {
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
   public static void blockUntilViewsReceived(long timeout, Cache... caches) {
      blockUntilViewsReceived(caches, timeout);
   }

   /**
    * Version of blockUntilViewsReceived that throws back any interruption
    */
   public static void blockUntilViewsReceivedInt(long timeout, Cache... caches) throws InterruptedException {
      blockUntilViewsReceivedInt(caches, timeout);
   }

   /**
    * Version of blockUntilViewsReceived that uses varargsa and cache managers
    */
   public static void blockUntilViewsReceived(long timeout, CacheContainer... cacheContainers) {
      blockUntilViewsReceived(timeout, true, cacheContainers);
   }

   /**
    * Waits for the given memebrs to be removed from the cluster. The difference between this and {@link
    * #blockUntilViewsReceived(long, org.infinispan.manager.CacheContainer...)} methods(s) is that it does not barf if
    * more than expected memebers is in the cluster - this is because we expect to start with a grater number fo
    * memebers than we eventually expect. It will barf though, if the number of members is not the one expected but only
    * after the timeout expieres.
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
    * An overloaded version of {@link #blockUntilViewsReceived(long,Cache[])} that allows for 'shrinking' clusters.
    * I.e., the usual method barfs if there are more members than expected.  This one takes a param
    * (barfIfTooManyMembers) which, if false, will NOT barf but will wait until the cluster 'shrinks' to the desired
    * size.  Useful if in tests, you kill a member and want to wait until this fact is known across the cluster.
    *
    * @param timeout
    * @param barfIfTooManyMembers
    * @param caches
    */
   public static void blockUntilViewsReceived(long timeout, boolean barfIfTooManyMembers, Cache... caches) {
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
    *
    * @throws RuntimeException if <code>timeout</code> ms have elapse without all caches having the same number of
    *                          members.
    */
   public static void blockUntilViewReceived(Cache cache, int groupSize, long timeout) {
      blockUntilViewReceived(cache, groupSize, timeout, true);
   }

   /**
    * Loops, continually calling {@link #areCacheViewsComplete(Cache[])} until
    * it either returns true or a default timeout has elapsed.
    *
    * @param groupSize number of caches expected in the group
    */
   public static void blockUntilViewReceived(Cache cache, int groupSize) {
      // Default 10 seconds
      blockUntilViewReceived(cache, groupSize, 10000, true);
   }

   public static void blockUntilViewReceived(Cache cache, int groupSize, long timeout, boolean barfIfTooManyMembersInView) {
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
    *
    * @return <code>true</code> if all caches have <code>caches.length</code> members; false otherwise
    *
    * @throws IllegalStateException if any of the caches have MORE view members than caches.length
    */
   public static boolean areCacheViewsComplete(Cache[] caches) {
      return areCacheViewsComplete(caches, true);
   }

   public static boolean areCacheViewsComplete(Cache[] caches, boolean barfIfTooManyMembers) {
      int memberCount = caches.length;

      for (int i = 0; i < memberCount; i++) {
         EmbeddedCacheManager cacheManager = caches[i].getCacheManager();
         if (!isCacheViewComplete(cacheManager.getMembers(), cacheManager.getAddress(), memberCount, barfIfTooManyMembers)) {
            return false;
         }
      }

      return true;
   }

   public static boolean areCacheViewsComplete(boolean barfIfTooManyMembers, CacheContainer... cacheContainers) {
      if (cacheContainers == null) throw new NullPointerException("Cache Manager array is null");
      int memberCount = cacheContainers.length;

      for (int i = 0; i < memberCount; i++) {
         EmbeddedCacheManager cacheManager = (EmbeddedCacheManager) cacheContainers[i];
         if (!isCacheViewComplete(cacheManager.getMembers(), cacheManager.getAddress(), memberCount, barfIfTooManyMembers)) {
            return false;
         }
      }

      return true;
   }

   /**
    * @param c
    * @param memberCount
    */
   public static boolean isCacheViewComplete(Cache c, int memberCount) {
      EmbeddedCacheManager cacheManager = c.getCacheManager();
      return isCacheViewComplete(cacheManager.getMembers(), cacheManager.getAddress(), memberCount, true);
   }

   public static boolean isCacheViewComplete(List members, Address address, int memberCount, boolean barfIfTooManyMembers) {
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
    * @param timeout max number of milliseconds to block for
    * @param finalViewSize desired final view size
    * @param caches caches representing current, or expected members in the cluster.
    */
   public static void blockUntilViewsChanged(long timeout, int finalViewSize, Cache... caches) {
      blockUntilViewsChanged(caches, timeout, finalViewSize);
   }

   private static void blockUntilViewsChanged(Cache[] caches, long timeout, int finalViewSize) {
      long failTime = System.currentTimeMillis() + timeout;

      while (System.currentTimeMillis() < failTime) {
         sleepThread(100);
         if (areCacheViewsChanged(caches, finalViewSize)) {
            return;
         }
      }

      List<List<Address>> allViews = new ArrayList<List<Address>>(caches.length);
      for (int i = 0; i < caches.length; i++) {
         allViews.add(caches[i].getCacheManager().getMembers());
      }

      throw new RuntimeException(String.format(
            "Timed out before caches had changed views (%s) to contain %d members",
            allViews, finalViewSize));
   }

   private static boolean areCacheViewsChanged(Cache[] caches, int finalViewSize) {
      int memberCount = caches.length;

      for (int i = 0; i < memberCount; i++) {
         EmbeddedCacheManager cacheManager = caches[i].getCacheManager();
         if (!isCacheViewChanged(cacheManager.getMembers(), finalViewSize)) {
            return false;
         }
      }

      return true;
   }

   private static boolean isCacheViewChanged(List members, int finalViewSize) {
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
      }
      catch (InterruptedException ie) {
         if (messageOnInterrupt != null)
            log.error(messageOnInterrupt);
      }
   }

   public static void sleepThreadInt(long sleeptime, String messageOnInterrupt) throws InterruptedException {
      try {
         Thread.sleep(sleeptime);
      }
      catch (InterruptedException ie) {
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

   public static void killCacheManagers(List<? extends CacheContainer> cacheContainers) {
      EmbeddedCacheManager[] cms = new EmbeddedCacheManager[cacheContainers.size()];
      for (int i = 0; i < cacheContainers.size(); i++) cms[i] = (EmbeddedCacheManager) cacheContainers.get(i);
      killCacheManagers(cms);
   }

   public static void killCacheManagers(EmbeddedCacheManager... cacheManagers) {
      killCacheManagers(false, cacheManagers);
   }

   public static void killCacheManagers(boolean clear, EmbeddedCacheManager... cacheManagers) {
      // stop the caches first so that stopping the cache managers doesn't trigger a rehash
      for (EmbeddedCacheManager cm : cacheManagers) {
         try {
            killCaches(clear, getRunningCaches(cm));
         } catch (Throwable e) {
            log.warn("Problems stopping cache manager " + cm, e);
         }
      }
      // Stop the managers in reverse order to prevent each of them from becoming coordinator in turn
      for (int i = cacheManagers.length - 1; i >= 0; i--) {
         EmbeddedCacheManager cm = cacheManagers[i];
         try {
            if (cm != null)
               cm.stop();
         } catch (Throwable e) {
            log.warn("Problems killing cache manager " + cm, e);
         }
      }
   }

   public static void clearContent(EmbeddedCacheManager... cacheManagers) {
      clearContent(Arrays.asList(cacheManagers));
   }

   public static void clearContent(List<? extends EmbeddedCacheManager> cacheManagers) {
      for (EmbeddedCacheManager cm : cacheManagers) {
         try {
            clearContent(cm);
         } catch (Throwable e) {
            log.warn("Problems clearing cache manager " + cm, e);
         }
      }
   }

   public static void clearContent(EmbeddedCacheManager cacheContainer) {
      if (cacheContainer != null && cacheContainer.getStatus().allowInvocations()) {
         Set<Cache> runningCaches = getRunningCaches(cacheContainer);
         for (Cache cache : runningCaches) {
            clearRunningTx(cache);
         }

         if (!cacheContainer.getStatus().allowInvocations()) return;

         for (Cache cache : runningCaches) {
            clearCacheLoader(cache);
            removeInMemoryData(cache);
         }
      }
   }

   private static Set<String> getOrderedCacheNames(EmbeddedCacheManager cacheContainer) {
      Set<String> caches = new LinkedHashSet<>();
      try {
         DependencyGraph graph = TestingUtil.extractField(cacheContainer, "cacheDependencyGraph");
         caches.addAll(graph.topologicalSort());
      } catch (Exception ignored) {
      }
      return caches;
   }

   protected static Set<Cache> getRunningCaches(EmbeddedCacheManager cacheContainer) {
      if (cacheContainer == null || !cacheContainer.getStatus().allowInvocations())
         return new HashSet<>();

      Set<String> running = new LinkedHashSet<>();
      running.addAll(getOrderedCacheNames(cacheContainer));
      running.addAll(cacheContainer.getCacheNames());
      running.add(DEFAULT_CACHE_NAME);

      return running.stream()
              .map(s -> cacheContainer.getCache(s, false))
              .filter(Objects::nonNull)
              .filter(c -> c.getStatus().allowInvocations())
              .collect(Collectors.toCollection(LinkedHashSet::new));
   }

   private static void clearRunningTx(Cache cache) {
      if (cache != null) {
         TransactionManager txm = TestingUtil.getTransactionManager(cache);
         if (txm == null) return;
         try {
            if (txm.getTransaction() == null) return;
            txm.rollback();
         }
         catch (Exception e) {
            // don't care
         }
      }
   }

   public static void clearCacheLoader(Cache cache) {
      PersistenceManager persistenceManager = TestingUtil.extractComponent(cache, PersistenceManager.class);
      persistenceManager.clearAllStores(BOTH);
   }

   public static <K, V> List<CacheLoader<K, V>> cachestores(List<Cache<K, V>> caches) {
      List<CacheLoader<K, V>> l = new LinkedList<CacheLoader<K, V>>();
      for (Cache<K, V> c: caches)
         l.add(TestingUtil.getFirstLoader(c));
      return l;
   }

   private static void removeInMemoryData(Cache cache) {
      EmbeddedCacheManager mgr = cache.getCacheManager();
      Address a = mgr.getAddress();
      String str;
      if (a == null)
         str = "a non-clustered cache manager";
      else
         str = "a cache manager at address " + a;
      log.debugf("Cleaning data for cache '%s' on %s", cache.getName(), str);
      DataContainer dataContainer = TestingUtil.extractComponent(cache, DataContainer.class);
      if (log.isDebugEnabled()) log.debugf("Data container size before clear: %d", dataContainer.size());
      dataContainer.clear();
   }

   /**
    * Kills a cache - stops it, clears any data in any stores, and rolls back any associated txs
    */
   public static void killCaches(Cache... caches) {
      killCaches(Arrays.asList(caches));
   }

   /**
    * Kills a cache - stops it and rolls back any associated txs
    */
   public static void killCaches(Collection<Cache> caches) {
      killCaches(false, caches);
   }

   /**
    * Kills a cache - stops it and rolls back any associated txs
    */
   public static void killCaches(boolean clear, Collection<Cache> caches) {
      for (Cache c : caches) {
         try {
            if (c != null && c.getStatus() == ComponentStatus.RUNNING) {
               TransactionManager tm = c.getAdvancedCache().getTransactionManager();
               if (tm != null) {
                  try {
                     tm.rollback();
                  }
                  catch (Exception e) {
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
               if (clear) {
                  try {
                     c.clear();
                  } catch (Exception ignored) {}
               }
               c.stop();
            }
         }
         catch (Throwable t) {

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
            txManager.rollback();
         }
         catch (Exception e) {
            // don't care
         }
      }
   }


   /**
    * Clears any associated transactions with the current thread in the caches' transaction managers.
    */
   public static void killTransactions(Cache... caches) {
      for (Cache c : caches) {
         if (c != null && c.getStatus() == ComponentStatus.RUNNING) {
            TransactionManager tm = getTransactionManager(c);
            if (tm != null) {
               try {
                  tm.rollback();
               }
               catch (Exception e) {
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
    *
    * @return component registry
    */
   public static ComponentRegistry extractComponentRegistry(Cache cache) {
      ComponentRegistry cr = cache.getAdvancedCache().getComponentRegistry();
      return cr;
   }

   public static GlobalComponentRegistry extractGlobalComponentRegistry(CacheContainer cacheContainer) {
      return ((EmbeddedCacheManager) cacheContainer).getGlobalComponentRegistry();
   }

   public static LockManager extractLockManager(Cache cache) {
      return extractComponentRegistry(cache).getComponent(LockManager.class);
   }

   public static StreamingMarshaller extractGlobalMarshaller(EmbeddedCacheManager cm) {
      GlobalComponentRegistry gcr = extractField(cm, "globalComponentRegistry");
      return gcr.getComponent(StreamingMarshaller.class);
   }

   public static ExternalizerTable extractExtTable(CacheContainer cacheContainer) {
      GlobalComponentRegistry gcr = extractField(cacheContainer, "globalComponentRegistry");
      return gcr.getComponent(ExternalizerTable.class);
   }

   /**
    * Replaces an existing interceptor of the given type in the interceptor chain with a new interceptor instance passed
    * as parameter.
    *
    * @param replacingInterceptor        the interceptor to add to the interceptor chain
    * @param toBeReplacedInterceptorType the type of interceptor that should be swapped with the new one
    *
    * @return true if the interceptor was replaced
    */
   public static boolean replaceInterceptor(Cache cache, CommandInterceptor replacingInterceptor, Class<? extends CommandInterceptor> toBeReplacedInterceptorType) {
      ComponentRegistry cr = extractComponentRegistry(cache);
      // make sure all interceptors here are wired.
      CommandInterceptor i = replacingInterceptor;
      do {
         cr.wireDependencies(i);
      }
      while ((i = i.getNext()) != null);
      AsyncInterceptorChain inch = cache.getAdvancedCache().getAsyncInterceptorChain();
      return inch.replaceInterceptor(replacingInterceptor, toBeReplacedInterceptorType);
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
   public static boolean replaceInterceptor(Cache cache, AsyncInterceptor replacingInterceptor, Class<? extends AsyncInterceptor> toBeReplacedInterceptorType) {
      ComponentRegistry cr = extractComponentRegistry(cache);
      // make sure all interceptors here are wired.
      cr.wireDependencies(replacingInterceptor);
      AsyncInterceptorChain inch = cr.getComponent(AsyncInterceptorChain.class);
      return inch.replaceInterceptor(replacingInterceptor, toBeReplacedInterceptorType);
   }

   /**
    * Retrieves the remote delegate for a given cache.  It is on this remote delegate that the JGroups RPCDispatcher
    * invokes remote methods.
    *
    * @param cache cache instance for which a remote delegate is to be retrieved
    *
    * @return remote delegate, or null if the cacge is not configured for replication.
    */
   public static CacheImpl getInvocationDelegate(Cache cache) {
      return (CacheImpl) cache;
   }

   /**
    * Blocks until the cache has reached a specified state.
    *
    * @param cache       cache to watch
    * @param cacheStatus status to wait for
    * @param timeout     timeout to wait for
    */
   public static void blockUntilCacheStatusAchieved(Cache cache, ComponentStatus cacheStatus, long timeout) {
      AdvancedCache spi = cache.getAdvancedCache();
      long killTime = System.currentTimeMillis() + timeout;
      while (System.currentTimeMillis() < killTime) {
         if (spi.getStatus() == cacheStatus) return;
         sleepThread(50);
      }
      throw new RuntimeException("Timed out waiting for condition");
   }

   public static void replicateCommand(Cache cache, VisitableCommand command) throws Throwable {
      ComponentRegistry cr = extractComponentRegistry(cache);
      AsyncInterceptorChain ic = cr.getComponent(AsyncInterceptorChain.class);
      InvocationContextFactory icf = cr.getComponent(InvocationContextFactory.class);
      InvocationContext ctxt = icf.createInvocationContext(true, -1);
      ic.invoke(ctxt, command);
   }

   public static void blockUntilViewsReceived(int timeout, Collection caches) {
      Object first = caches.iterator().next();
      if (first instanceof Cache) {
         blockUntilViewsReceived(timeout, (Cache[]) caches.toArray(new Cache[]{}));
      } else {
         blockUntilViewsReceived(timeout, (CacheContainer[]) caches.toArray(new CacheContainer[]{}));
      }
   }

   public static void blockUntilViewsReceived(int timeout, boolean barfIfTooManyMembers, Collection caches) {
      Object first = caches.iterator().next();
      if (first instanceof Cache) {
         blockUntilViewsReceived(timeout, barfIfTooManyMembers, (Cache[]) caches.toArray(new Cache[]{}));
      } else {
         blockUntilViewsReceived(timeout, barfIfTooManyMembers, (CacheContainer[]) caches.toArray(new CacheContainer[]{}));
      }
   }

   public static CommandsFactory extractCommandsFactory(Cache<?, ?> cache) {
      return (CommandsFactory) extractField(cache, "commandsFactory");
   }

   public static void dumpCacheContents(List caches) {
      System.out.println("**** START: Cache Contents ****");
      int count = 1;
      for (Object o : caches) {
         Cache c = (Cache) o;
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

   public static void dumpCacheContents(Cache... caches) {
      dumpCacheContents(Arrays.asList(caches));
   }

   /**
    * Extracts a component of a given type from the cache's internal component registry
    */
   public static <T> T extractComponent(Cache cache, Class<T> componentType) {
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

   public static TransactionManager getTransactionManager(Cache cache) {
      return cache == null ? null : extractComponent(cache, TransactionManager.class);
   }

   /**
    * Replaces a component in a running cache
    *
    * @param cache                cache in which to replace component
    * @param componentType        component type of which to replace
    * @param replacementComponent new instance
    * @param rewire               if true, ComponentRegistry.rewire() is called after replacing.
    *
    * @return the original component that was replaced
    */
   public static <T> T replaceComponent(Cache<?, ?> cache, Class<T> componentType, T replacementComponent, boolean rewire) {
      ComponentRegistry cr = extractComponentRegistry(cache);
      T old = cr.getComponent(componentType);
      cr.registerComponent(replacementComponent, componentType);
      if (rewire) cr.rewire();
      return old;
   }

   /**
    * Replaces a component in a running cache manager (global component registry)
    *
    * @param cacheContainer       cache in which to replace component
    * @param componentType        component type of which to replace
    * @param replacementComponent new instance
    * @param rewire               if true, ComponentRegistry.rewire() is called after replacing.
    *
    * @return the original component that was replaced
    */
   public static <T> T replaceComponent(CacheContainer cacheContainer, Class<T> componentType, T replacementComponent, boolean rewire) {
      GlobalComponentRegistry cr = extractGlobalComponentRegistry(cacheContainer);
      T old = cr.getComponent(componentType);
      cr.registerComponent(replacementComponent, componentType);
      if (rewire) {
         cr.rewire();
         cr.rewireNamedRegistries();
      }
      return old;
   }

   public static CacheLoader getCacheLoader(Cache cache) {
      PersistenceManager clm = extractComponent(cache, PersistenceManager.class);
      if (cache.getCacheConfiguration().persistence().usingStores()) {
         return TestingUtil.getFirstLoader(cache);
      } else {
         return null;
      }
   }

   public static String printCache(Cache cache) {
      DataContainer dataContainer = TestingUtil.extractComponent(cache, DataContainer.class);
      Iterator it = dataContainer.iterator();
      StringBuilder builder = new StringBuilder(cache.getName() + "[");
      while (it.hasNext()) {
         CacheEntry ce = (CacheEntry) it.next();
         builder.append(ce.getKey() + "=" + ce.getValue() + ",l=" + ce.getLifespan() + "; ");
      }
      builder.append("]");
      return builder.toString();
   }

   public static <K> Set<K> getInternalKeys(Cache<K, ?> cache) {
      DataContainer<K, ?> dataContainer = TestingUtil.extractComponent(cache, DataContainer.class);
      Set<K> keys = new HashSet<K>();
      for (CacheEntry<K, ?> entry : dataContainer) {
         keys.add(entry.getKey());
      }
      return keys;
   }

   public static <V> Collection<V> getInternalValues(Cache<?, V> cache) {
      DataContainer<?, V> dataContainer = TestingUtil.extractComponent(cache, DataContainer.class);
      Collection<V> values = new ArrayList<V>();
      for (CacheEntry<?, V> entry : dataContainer) {
         values.add(entry.getValue());
      }
      return values;
   }

   public static DISCARD getDiscardForCache(Cache<?, ?> c) throws Exception {
      JGroupsTransport jgt = (JGroupsTransport) TestingUtil.extractComponent(c, Transport.class);
      JChannel ch = jgt.getChannel();
      ProtocolStack ps = ch.getProtocolStack();
      DISCARD discard = new DISCARD();
      ps.insertProtocol(discard, ProtocolStack.Position.ABOVE, TP.class);
      return discard;
   }

   /**
    * Inserts a DELAY protocol in the JGroups stack used by the cache, and returns it.
    * The DELAY protocol can then be used to inject delays in milliseconds both at receiver
    * and sending side.
    * @param cache
    * @param in_delay_millis
    * @param out_delay_millis
    * @return a reference to the DELAY instance being used by the JGroups stack
    * @throws Exception
    */
   public static DELAY setDelayForCache(Cache<?, ?> cache, int in_delay_millis, int out_delay_millis) throws Exception {
      JGroupsTransport jgt = (JGroupsTransport) TestingUtil.extractComponent(cache, Transport.class);
      JChannel ch = jgt.getChannel();
      ProtocolStack ps = ch.getProtocolStack();
      DELAY delay = ps.findProtocol(DELAY.class);
      if (delay==null) {
         delay = new DELAY();
         ps.insertProtocol(delay, ProtocolStack.Position.ABOVE, TP.class);
      }
      delay.setInDelay(in_delay_millis);
      delay.setOutDelay(out_delay_millis);
      return delay;
   }

   /**
    * Creates a path to a unique (per test) temporary directory.
    * By default, the directory is created in the platform's temp directory, but the location
    * can be overridden with the {@code infinispan.test.tmpdir} system property.
    *
    * @param test  test that requires this directory.
    *
    * @return an absolute path
    */
   public static String tmpDirectory(Class<?> test) {
      String prefix = System.getProperty("infinispan.test.tmpdir", System.getProperty("java.io.tmpdir"));
      return prefix + separator + TEST_PATH + separator + test.getSimpleName();
   }

   /**
    * See {@link #tmpDirectory(Class)}
    *
    * @return an absolute path
    */
   public static String tmpDirectory(String folder) {
      String prefix = System.getProperty("infinispan.test.tmpdir", System.getProperty("java.io.tmpdir"));
      return prefix + separator + TEST_PATH + separator + folder;
   }

   public static String k(Method method, int index) {
      return new StringBuilder().append("k").append(index).append('-')
              .append(method.getName()).toString();
   }

   public static String v(Method method, int index) {
      return new StringBuilder().append("v").append(index).append('-')
              .append(method.getName()).toString();
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

   public static TransactionTable getTransactionTable(Cache<?, ?> cache) {
      return cache.getAdvancedCache().getComponentRegistry().getComponent(TransactionTable.class);
   }

   public static String getMethodSpecificJmxDomain(Method m, String jmxDomain) {
      return jmxDomain + '.' + m.getName();
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
      } catch (Exception e) {
         throw new RuntimeException(e);
      }
   }

   public static ObjectName getCacheObjectName(String jmxDomain) {
      return getCacheObjectName(jmxDomain, DEFAULT_CACHE_NAME + "(local)");
   }

   public static ObjectName getCacheObjectName(String jmxDomain, String cacheName) {
      return getCacheObjectName(jmxDomain, cacheName, "Cache");
   }

   public static ObjectName getCacheObjectName(String jmxDomain, String cacheName, String component) {
      return getCacheObjectName(jmxDomain, cacheName, component, "DefaultCacheManager");
   }

   public static ObjectName getCacheObjectName(String jmxDomain, String cacheName, String component, String cacheManagerName) {
      try {
         return new ObjectName(jmxDomain + ":type=Cache,manager=" + ObjectName.quote(cacheManagerName)
               + ",name=" + ObjectName.quote(cacheName) + ",component=" + component);
      } catch (Exception e) {
         throw new RuntimeException(e);
      }
   }

   public static ObjectName getJGroupsChannelObjectName(String jmxDomain, String clusterName) throws Exception {
      return new ObjectName(String.format("%s:type=channel,cluster=%s", jmxDomain, ObjectName.quote(clusterName)));
   }

   public static boolean existsObject(ObjectName objectName) {
      return PerThreadMBeanServerLookup.getThreadMBeanServer().isRegistered(objectName);
   }

   public static boolean existsDomains(String... domains) {
      MBeanServer mBeanServer = PerThreadMBeanServerLookup.getThreadMBeanServer();
      Set<String> domainSet = new HashSet<String>(Arrays.asList(domains));
      for (String domain : mBeanServer.getDomains()) {
         if (domainSet.contains(domain)) return true;
      }
      return false;
   }

   public static void checkMBeanOperationParameterNaming(ObjectName objectName) throws Exception {
      MBeanServer mBeanServer = PerThreadMBeanServerLookup.getThreadMBeanServer();
      MBeanInfo mBeanInfo = mBeanServer.getMBeanInfo(objectName);
      for(MBeanOperationInfo op : mBeanInfo.getOperations()) {
         for(MBeanParameterInfo param : op.getSignature()) {
            assertFalse(param.getName().matches("p[0-9]+"));
         }
      }
   }

   public static String generateRandomString(int numberOfChars) {
      Random r = new Random(System.currentTimeMillis());
      StringBuilder sb = new StringBuilder();
      for (int i = 0; i < numberOfChars; i++) sb.append((char) (64 + r.nextInt(26)));
      return sb.toString();
   }

   /**
    * Verifies the cache doesn't contain any lock
    * @param cache
    */
   public static void assertNoLocks(Cache<?,?> cache) {
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
    * @param tm transaction manager
    * @param c callable instance to run within a transaction
    * @param <T> type returned from the callable
    * @return returns whatever the callable returns
    * @throws Exception
    */
   public static <T> T withTx(TransactionManager tm, Callable<T> c) throws Exception {
      return withTxCallable(tm, c).call();
   }

   /**
    * Returns a callable that will call the provided callable within a transaction.  This method guarantees that the
    * right pattern is used to make sure that the transaction is always either committed or rollbacked around
    * the callable.
    *
    * @param tm transaction manager
    * @param c callable instance to run within a transaction
    * @param <T> tyep of callable to return
    * @return The callable to invoke.  Note as long as the provided callable is thread safe this callable will be as well
    */
   public static <T> Callable<T> withTxCallable(final TransactionManager tm, final Callable<? extends T> c) {
      return new Callable<T>() {
         @Override
         public T call() throws Exception {
            tm.begin();
            try {
               return c.call();
            } catch (Exception e) {
               tm.setRollbackOnly();
               throw e;
            } finally {
               if (tm.getStatus() == Status.STATUS_ACTIVE) tm.commit();
               else tm.rollback();
            }
         }
      };
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
         TestingUtil.killCacheManagers(c.clearBeforeKill(), c.cm);
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
         if (cm != null) TestingUtil.killCacheManagers(false, cm);
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

   public static InternalMetadataImpl internalMetadata(Long lifespan, Long maxIdle) {
      long now = System.currentTimeMillis();
      return new InternalMetadataImpl(metadata(lifespan, maxIdle), now, now);
   }


   public static <T extends CacheLoader<K, V>, K, V>  T getFirstLoader(Cache<K, V> cache) {
      PersistenceManagerImpl persistenceManager = (PersistenceManagerImpl) extractComponent(cache, PersistenceManager.class);
      //noinspection unchecked
      return (T) persistenceManager.getAllLoaders().get(0);
   }

   @SuppressWarnings("unchecked")
   public static <T extends CacheWriter<K, V>, K, V> T getFirstWriter(Cache<K, V> cache) {
      PersistenceManagerImpl persistenceManager = (PersistenceManagerImpl) extractComponent(cache, PersistenceManager.class);
      return (T) persistenceManager.getAllWriters().get(0);
   }

   @SuppressWarnings("unchecked")
   public static <T extends CacheWriter<K, V>, K, V> T getFirstTxWriter(Cache<K, V> cache) {
      PersistenceManagerImpl persistenceManager = (PersistenceManagerImpl) extractComponent(cache, PersistenceManager.class);
      return (T) persistenceManager.getAllTxWriters().get(0);
   }

   public static Set<MarshalledEntry> allEntries(AdvancedLoadWriteStore cl, KeyFilter filter) {
      final Set<MarshalledEntry> result = new HashSet<MarshalledEntry>();
      cl.process(filter, new AdvancedCacheLoader.CacheLoaderTask() {
         @Override
         public void processEntry(MarshalledEntry marshalledEntry, AdvancedCacheLoader.TaskContext taskContext) throws InterruptedException {
            result.add(marshalledEntry);
         }
      }, new WithinThreadExecutor(), true, true);
      return result;
   }

   public static Set<MarshalledEntry> allEntries(AdvancedLoadWriteStore cl) {
      return allEntries(cl, null);
   }

   public static MarshalledEntry marshalledEntry(InternalCacheEntry ice, StreamingMarshaller marshaller) {
      return new MarshalledEntryImpl(ice.getKey(), ice.getValue(), PersistenceUtil.internalMetadata(ice), marshaller);
   }

   public static MarshalledEntry marshalledEntry(InternalCacheValue icv, StreamingMarshaller marshaller) {
      return marshalledEntry(icv, marshaller);
   }

   public static void outputPropertiesToXML(String outputFile, Properties properties) throws IOException {
      Properties sorted = new Properties() {
         @Override
         public Set<Object> keySet() {
            return Collections.unmodifiableSet(new TreeSet<Object>(super.keySet()));
         }

         @Override
         public synchronized Enumeration<Object> keys() {
            return Collections.enumeration(new TreeSet<Object>(super.keySet()));
         }

         @Override
         public Set<String> stringPropertyNames() {
            return Collections.unmodifiableSet(new TreeSet<String>(super.stringPropertyNames()));
         }
      };
      sorted.putAll(properties);
      OutputStream stream = new FileOutputStream(outputFile);
      try {
         sorted.storeToXML(stream, null);
      } finally {
         stream.close();
      }
   }

   public static <K, V> void writeToAllStores(K key, V value, Cache<K, V> cache) {
      AdvancedCache<K, V> advCache = cache.getAdvancedCache();
      PersistenceManager pm = advCache.getComponentRegistry().getComponent(PersistenceManager.class);
      StreamingMarshaller marshaller = extractGlobalMarshaller(advCache.getCacheManager());
      pm.writeToAllNonTxStores(new MarshalledEntryImpl(key, value, null, marshaller), BOTH);
   }

   public static <K, V> boolean deleteFromAllStores(K key, Cache<K, V> cache) {
      AdvancedCache<K, V> advCache = cache.getAdvancedCache();
      PersistenceManager pm = advCache.getComponentRegistry().getComponent(PersistenceManager.class);
      return pm.deleteFromAllStores(key, BOTH);
   }

   public static Subject makeSubject(String... principals) {
      Set<Principal> set = new HashSet<Principal>();
      for (String principal : principals) {
         set.add(new TestingUtil.TestPrincipal(principal));
      }
      return new Subject(true, set, Collections.emptySet(), Collections.emptySet());
   }

   public static String loadFileAsString(InputStream is) throws IOException {
      StringBuilder sb = new StringBuilder();
      BufferedReader r = new BufferedReader(new InputStreamReader(is));
      for (String line = r.readLine(); line != null; line = r.readLine()) {
         sb.append(line);
         sb.append("\n");
      }
      return sb.toString();
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
            if (other.name != null)
               return false;
         } else if (!name.equals(other.name))
            return false;
         return true;
      }
   }

   public static <T, W extends T> W wrapGlobalComponent(CacheContainer cacheContainer, Class<T> tClass,
                                                        WrapFactory<T, W, CacheContainer> factory, boolean rewire) {
      T current = extractGlobalComponent(cacheContainer, tClass);
      W wrap = factory.wrap(cacheContainer, current);
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

   public static <T extends PerCacheInboundInvocationHandler> T wrapPerCacheInboundInvocationHandler(
         Cache<?, ?> cache, WrapFactory<PerCacheInboundInvocationHandler, T, Cache<?, ?>> factory, boolean rewire) {
      PerCacheInboundInvocationHandler current = extractComponent(cache, PerCacheInboundInvocationHandler.class);
      T wrap = factory.wrap(cache, current);
      replaceComponent(cache, PerCacheInboundInvocationHandler.class, wrap, rewire);
      replaceField(wrap, "inboundInvocationHandler", cache.getAdvancedCache().getComponentRegistry(), ComponentRegistry.class);
      return wrap;
   }

   public interface WrapFactory<T, W, C> {
      W wrap(C wrapOn, T current);
   }

   public static void expectCause(Throwable t, Class<? extends Throwable> c, String messageRegex) throws Exception {
      for (;;) {
         if (c.isAssignableFrom(t.getClass())) {
            if (messageRegex != null && !Pattern.matches(messageRegex, t.getMessage())) {
               throw new RuntimeException(String.format("Exception message '%s' does not match regex '%s'", t.getMessage(), messageRegex));
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

   public static void detectThreadLeaks(String regexp) {
      List<Thread> leakedThreads = new ArrayList<>();
      for (Map.Entry<Thread, StackTraceElement[]> s : Thread.getAllStackTraces().entrySet()) {
         Thread thread = s.getKey();
         if (thread.getName().matches(regexp)) leakedThreads.add(thread);
      }

      if (!leakedThreads.isEmpty())
         throw new AssertionError("Leaked threads: " + leakedThreads);
   }

}
