/*
 * JBoss, Home of Professional Open Source
 * Copyright 2011 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */

package org.infinispan.test;

import static java.io.File.separator;
import static org.testng.AssertJUnit.assertFalse;

import java.io.File;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import javax.management.MBeanInfo;
import javax.management.MBeanOperationInfo;
import javax.management.MBeanParameterInfo;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.transaction.Status;
import javax.transaction.TransactionManager;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.CacheImpl;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.InvocationContextContainer;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.interceptors.InterceptorChain;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.jmx.PerThreadMBeanServerLookup;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.loaders.CacheLoader;
import org.infinispan.loaders.CacheLoaderManager;
import org.infinispan.loaders.CacheStore;
import org.infinispan.manager.CacheContainer;
import org.infinispan.manager.CacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.marshall.AbstractDelegatingMarshaller;
import org.infinispan.marshall.StreamingMarshaller;
import org.infinispan.marshall.jboss.ExternalizerTable;
import org.infinispan.remoting.ReplicationQueue;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.remoting.transport.jgroups.JGroupsTransport;
import org.infinispan.statetransfer.StateTransferManager;
import org.infinispan.topology.CacheTopology;
import org.infinispan.topology.DefaultRebalancePolicy;
import org.infinispan.topology.RebalancePolicy;
import org.infinispan.transaction.TransactionTable;
import org.infinispan.util.concurrent.locks.LockManager;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.jgroups.Channel;
import org.jgroups.protocols.DELAY;
import org.jgroups.protocols.DISCARD;
import org.jgroups.protocols.TP;
import org.jgroups.stack.ProtocolStack;

public class TestingUtil {

   private static final Log log = LogFactory.getLog(TestingUtil.class);
   private static final Random random = new Random();
   public static final String TEST_PATH = "infinispanTempFiles";
   public static final String INFINISPAN_START_TAG = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<infinispan\n" +
         "      xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"\n" +
         "      xsi:schemaLocation=\"urn:infinispan:config:5.3 http://www.infinispan.org/schemas/infinispan-config-5.3.xsd\"\n" +
         "      xmlns=\"urn:infinispan:config:5.3\">";
   public static final String INFINISPAN_START_TAG_52 = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<infinispan\n" +
         "      xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"\n" +
         "      xsi:schemaLocation=\"urn:infinispan:config:5.2 http://www.infinispan.org/schemas/infinispan-config-5.2.xsd\"\n" +
         "      xmlns=\"urn:infinispan:config:5.2\">";
   public static final String INFINISPAN_START_TAG_51 = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<infinispan\n" +
         "      xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"\n" +
         "      xsi:schemaLocation=\"urn:infinispan:config:5.1 http://www.infinispan.org/schemas/infinispan-config-5.1.xsd\"\n" +
         "      xmlns=\"urn:infinispan:config:5.1\">";
   public static final String INFINISPAN_START_TAG_40 = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<infinispan\n" +
           "      xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"\n" +
           "      xsi:schemaLocation=\"urn:infinispan:config:4.0 http://www.infinispan.org/schemas/infinispan-config-4.0.xsd\"\n" +
           "      xmlns=\"urn:infinispan:config:4.0\">";
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
   public static Object extractField(Object target, String fieldName) {
      return extractField(target.getClass(), target, fieldName);
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
               e.printStackTrace();
               return null;
            } else {
               // try with superclass!!
               type = type.getSuperclass();
            }
         }
      }
   }

   public static <T extends CommandInterceptor> T findInterceptor(Cache<?, ?> cache, Class<T> interceptorToFind) {
      for (CommandInterceptor i : cache.getAdvancedCache().getInterceptorChain()) {
         if (interceptorToFind.isInstance(i)) return interceptorToFind.cast(i);
      }
      return null;
   }

   public static void waitForRehashToComplete(Cache... caches) {
      int gracetime = 90000; // 90 seconds
      long giveup = System.currentTimeMillis() + gracetime;
      for (Cache c : caches) {
         StateTransferManager stateTransferManager = extractComponent(c, StateTransferManager.class);
         DefaultRebalancePolicy rebalancePolicy = (DefaultRebalancePolicy) TestingUtil.extractGlobalComponent(c.getCacheManager(), RebalancePolicy.class);
         Address cacheAddress = c.getAdvancedCache().getRpcManager().getAddress();
         while (true) {
            CacheTopology cacheTopology = stateTransferManager.getCacheTopology();
            boolean rebalanceInProgress = stateTransferManager.isStateTransferInProgress();
            boolean chIsBalanced = !rebalanceInProgress && rebalancePolicy.isBalanced(cacheTopology.getCurrentCH());
            boolean chContainsAllMembers = cacheTopology.getCurrentCH().getMembers().size() == caches.length;
            if (chIsBalanced && chContainsAllMembers)
               break;

            if (System.currentTimeMillis() > giveup) {
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
                        "current topology is %s", c.getCacheManager().getAddress(), cacheTopology);
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
      List<List<Address>> allViews = new ArrayList<List<Address>>(length);
      for (int i = 0; i < length; i++) {
         EmbeddedCacheManager cm = (EmbeddedCacheManager) cacheContainers[i];
         allViews.add(cm.getMembers());
      }

      throw new RuntimeException(String.format(
         "Timed out before caches had complete views.  Expected %d members in each view.  Views are as follows: %s",
         cacheContainers.length, allViews));
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
      if (members == null || finalViewSize != members.size())
         return false;
      else
         return true;
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

   public static void recursiveFileRemove(String directoryName) {
      File file = new File(directoryName);
      recursiveFileRemove(file);
   }

   public static void recursiveFileRemove(File file) {
      if (file.exists()) {
         log.tracef("Deleting file %s", file);
         recursiveDelete(file);
      }
   }

   private static void recursiveDelete(File f) {
      File absoluteFile = f.getAbsoluteFile();
      if (absoluteFile.isDirectory()) {
         File[] files = absoluteFile.listFiles();
         if (files != null) {
            for (File file : files) {
               recursiveDelete(file);
            }
         }
      }
      absoluteFile.delete();
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
      // stop the caches first so that stopping the cache managers doesn't trigger a rehash
      for (EmbeddedCacheManager cm : cacheManagers) {
         try {
            killCaches(getRunningCaches(cm));
         } catch (Throwable e) {
            log.warn("Problems stopping cache manager " + cm, e);
         }
      }
      for (EmbeddedCacheManager cm : cacheManagers) {
         try {
            if (cm != null) cm.stop();
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
            clearReplicationQueues(cache);
            clearCacheLoader(cache);
            removeInMemoryData(cache);
         }
      }
   }

   protected static Set<Cache> getRunningCaches(EmbeddedCacheManager cacheContainer) {
      Set<Cache> running = new HashSet<Cache>();
      if (cacheContainer == null || !cacheContainer.getStatus().allowInvocations())
         return running;

      for (String cacheName : cacheContainer.getCacheNames()) {
         if (cacheContainer.isRunning(cacheName)) {
            Cache c = cacheContainer.getCache(cacheName);
            if (c.getStatus().allowInvocations()) running.add(c);
         }
      }

      if (cacheContainer.isDefaultRunning()) {
         Cache defaultCache = cacheContainer.getCache();
         if (defaultCache.getStatus().allowInvocations()) running.add(defaultCache);
      }

      return running;
   }

   private static void clearRunningTx(Cache cache) {
      if (cache != null) {
         TransactionManager txm = TestingUtil.getTransactionManager(cache);
         if (txm == null) return;
         try {
            txm.rollback();
         }
         catch (Exception e) {
            // don't care
         }
      }
   }

   private static void clearReplicationQueues(Cache cache) {
      ReplicationQueue queue = TestingUtil.extractComponent(cache, ReplicationQueue.class);
      if (queue != null) queue.reset();
   }

   public static void clearCacheLoader(Cache cache) {
      CacheLoaderManager cacheLoaderManager = TestingUtil.extractComponent(cache, CacheLoaderManager.class);
      if (cacheLoaderManager != null && cacheLoaderManager.getCacheStore() != null) {
         try {
            cacheLoaderManager.getCacheStore().clear();
         } catch (Exception e) {
            throw new RuntimeException(e);
         }
      }
   }

   public static <K, V> List<CacheStore> cachestores(List<Cache<K, V>> caches) {
      List<CacheStore> l = new LinkedList<CacheStore>();
      for (Cache<?, ?> c: caches)
         l.add(TestingUtil.extractComponent(c, CacheLoaderManager.class).getCacheStore());
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
      if (log.isDebugEnabled()) log.debugf("removeInMemoryData(): dataContainerBefore == %s", dataContainer.entrySet());
      dataContainer.clear();
      if (log.isDebugEnabled()) log.debugf("removeInMemoryData(): dataContainerAfter == %s", dataContainer.entrySet());
   }

   /**
    * Kills a cache - stops it, clears any data in any cache loaders, and rolls back any associated txs
    */
   public static void killCaches(Cache... caches) {
      killCaches(Arrays.asList(caches));
   }

   /**
    * Kills a cache - stops it and rolls back any associated txs
    */
   public static void killCaches(Collection<Cache> caches) {
      for (Cache c : caches) {
         try {
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
               if (c.getAdvancedCache().getRpcManager() != null) {
                  log.tracef("Cache contents on %s before stopping: %s", c.getAdvancedCache().getRpcManager().getAddress(), c.entrySet());
               } else {
                  log.tracef("Cache contents before stopping: %s", c.entrySet());
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
      ComponentRegistry cr = (ComponentRegistry) extractField(cache, "componentRegistry");
      if (cr == null) cr = cache.getAdvancedCache().getComponentRegistry();
      return cr;
   }

   public static GlobalComponentRegistry extractGlobalComponentRegistry(CacheContainer cacheContainer) {
      return (GlobalComponentRegistry) extractField(cacheContainer, "globalComponentRegistry");
   }

   public static LockManager extractLockManager(Cache cache) {
      return extractComponentRegistry(cache).getComponent(LockManager.class);
   }

   /**
    * For testing only - introspects a cache and extracts the ComponentRegistry
    *
    * @param ci interceptor chain to introspect
    *
    * @return component registry
    */
   public static ComponentRegistry extractComponentRegistry(InterceptorChain ci) {
      return (ComponentRegistry) extractField(ci, "componentRegistry");
   }

   public static AbstractDelegatingMarshaller extractCacheMarshaller(Cache cache) {
      ComponentRegistry cr = (ComponentRegistry) extractField(cache, "componentRegistry");
      StreamingMarshaller marshaller = cr.getComponent(StreamingMarshaller.class, KnownComponentNames.CACHE_MARSHALLER);
      return (AbstractDelegatingMarshaller) marshaller;
   }

   public static AbstractDelegatingMarshaller extractGlobalMarshaller(EmbeddedCacheManager cm) {
      GlobalComponentRegistry gcr = (GlobalComponentRegistry) extractField(cm, "globalComponentRegistry");
      return (AbstractDelegatingMarshaller)
            gcr.getComponent(StreamingMarshaller.class, KnownComponentNames.GLOBAL_MARSHALLER);
   }

   public static ExternalizerTable extractExtTable(CacheContainer cacheContainer) {
      GlobalComponentRegistry gcr = (GlobalComponentRegistry) extractField(cacheContainer, "globalComponentRegistry");
      return gcr.getComponent(ExternalizerTable.class);
   }

   /**
    * Replaces the existing interceptor chain in the cache wih one represented by the interceptor passed in.  This
    * utility updates dependencies on all components that rely on the interceptor chain as well.
    *
    * @param cache       cache that needs to be altered
    * @param interceptor the first interceptor in the new chain.
    */
   public static void replaceInterceptorChain(Cache cache, CommandInterceptor interceptor) {
      ComponentRegistry cr = extractComponentRegistry(cache);
      // make sure all interceptors here are wired.
      CommandInterceptor i = interceptor;
      do {
         cr.wireDependencies(i);
      }
      while ((i = i.getNext()) != null);

      InterceptorChain inch = cr.getComponent(InterceptorChain.class);
      inch.setFirstInChain(interceptor);
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
      InterceptorChain inch = cr.getComponent(InterceptorChain.class);
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
      InterceptorChain ic = cr.getComponent(InterceptorChain.class);
      InvocationContextContainer icc = cr.getComponent(InvocationContextContainer.class);
      InvocationContext ctxt = icc.createInvocationContext(true, -1);
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

   public static CommandsFactory extractCommandsFactory(Cache<Object, Object> cache) {
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
      ComponentRegistry cr = extractComponentRegistry(cache);
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
      CacheLoaderManager clm = extractComponent(cache, CacheLoaderManager.class);
      if (clm != null && clm.isEnabled()) {
         return clm.getCacheLoader();
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

   public static Set getInternalKeys(Cache cache) {
      DataContainer dataContainer = TestingUtil.extractComponent(cache, DataContainer.class);
      Set keys = new HashSet();
      for (CacheEntry entry : dataContainer) {
         keys.add(entry.getKey());
      }
      return keys;
   }

   public static Collection getInternalValues(Cache cache) {
      DataContainer dataContainer = TestingUtil.extractComponent(cache, DataContainer.class);
      Collection values = new ArrayList();
      for (CacheEntry entry : dataContainer) {
         values.add(entry.getValue());
      }
      return values;
   }

   public static DISCARD getDiscardForCache(Cache<?, ?> c) throws Exception {
      JGroupsTransport jgt = (JGroupsTransport) TestingUtil.extractComponent(c, Transport.class);
      Channel ch = jgt.getChannel();
      ProtocolStack ps = ch.getProtocolStack();
      DISCARD discard = new DISCARD();
      ps.insertProtocol(discard, ProtocolStack.ABOVE, TP.class);
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
      Channel ch = jgt.getChannel();
      ProtocolStack ps = ch.getProtocolStack();
      DELAY delay = new DELAY();
      delay.setInDelay(in_delay_millis);
      delay.setOutDelay(out_delay_millis);
      ps.insertProtocol(delay, ProtocolStack.ABOVE, TP.class);
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
   public static String tmpDirectory(AbstractInfinispanTest test) {
      String prefix = System.getProperty("infinispan.test.tmpdir", System.getProperty("java.io.tmpdir"));
      return prefix + separator + TEST_PATH + separator + test.getClass().getSimpleName();
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

   public static TransactionTable getTransactionTable(Cache<Object, Object> cache) {
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
      return getCacheObjectName(jmxDomain, CacheContainer.DEFAULT_CACHE_NAME + "(local)");
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
      for (Object key : cache.keySet()) assert !lm.isLocked(key);
   }

   /**
    * Call an operation within a transaction. This method guarantees that the
    * right pattern is used to make sure that the transaction is always either
    * committed or rollbacked.
    *
    * @param tm transaction manager
    * @param c callable instance to run within a transaction
    * @param <T> type of callable return
    * @return returns whatever the callable returns
    * @throws Exception
    */
   public static <T> T withTx(TransactionManager tm, Callable<T> c) throws Exception {
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
      } finally {
         TestingUtil.killCacheManagers(c.cm);
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
}
