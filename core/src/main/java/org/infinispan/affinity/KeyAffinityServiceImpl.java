package org.infinispan.affinity;

import net.jcip.annotations.GuardedBy;
import net.jcip.annotations.ThreadSafe;
import org.infinispan.Cache;
import org.infinispan.distribution.ConsistentHash;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.notifications.cachemanagerlistener.event.ViewChangedEvent;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.concurrent.ReclosableLatch;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * // TODO: Document this
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
@ThreadSafe
public class KeyAffinityServiceImpl implements KeyAffinityService {

   private static Log log = LogFactory.getLog(KeyAffinityServiceImpl.class);

   private final Set<Address> filter;

   @GuardedBy("maxNumberInvariant")
   private final Map<Address, BlockingQueue> address2key = new ConcurrentHashMap<Address, BlockingQueue>();

   private final Executor executor;
   private final Cache cache;
   private final KeyGenerator keyGenerator;
   private final int bufferSize;
   private final AtomicInteger maxNumberOfKeys = new AtomicInteger(); //(nr. of addresses) * bufferSize;
   private final AtomicInteger exitingNumberOfKeys = new AtomicInteger();

   private volatile boolean started;

   /**
    * Guards and make sure the following invariant stands:  maxNumberOfKeys ==  address2key.keys().size() * bufferSize
    */
   private final ReadWriteLock maxNumberInvariant = new ReentrantReadWriteLock();


   /**
    * Used for coordinating between the KeyGeneratorWorker and consumers.
    */
   private final ReclosableLatch keyProducerStartLatch = new ReclosableLatch();


   public KeyAffinityServiceImpl(Executor executor, Cache cache, KeyGenerator keyGenerator, int bufferSize, Collection<Address> filter, boolean start) {
      this.executor = executor;
      this.cache = cache;
      this.keyGenerator = keyGenerator;
      this.bufferSize = bufferSize;
      if (filter != null) {
         this.filter = new CopyOnWriteArraySet<Address>();
      } else {
         this.filter = null;
      }
      if (start)
         start();
   }

   @Override
   public Object getCollocatedKey(Object otherKey) {
      Address address = getAddressForKey(otherKey);
      return getKeyForAddress(address);
   }

   @Override
   public Object getKeyForAddress(Address address) {
      if (!started) {
         throw new IllegalStateException("You have to start the service first!");
      }
      BlockingQueue queue = address2key.get(address);
      try {
         maxNumberInvariant.readLock().lock();
         Object result;
         try {
            result = queue.take();
         } finally {
            maxNumberInvariant.readLock().unlock();
         }
         exitingNumberOfKeys.decrementAndGet();
         return result;
      } catch (InterruptedException e) {
         Thread.currentThread().interrupt();
         return null;
      } finally {
         if (maxNumberOfKeys.equals(exitingNumberOfKeys)) {
            keyProducerStartLatch.open();
         }
      }
   }

   @Override
   public void start() {
      if (started) {
         log.info("Service already started, ignoring call to start!");
         return;
      }
      List<Address> existingNodes = getExistingNodes();
      maxNumberInvariant.writeLock().lock();
      try {
         addQueuesForAddresses(existingNodes);
         revisitNumberOfKeys();
      } finally {
         maxNumberInvariant.writeLock().unlock();
      }
      executor.execute(new KeyGeneratorWorker());
      cache.addListener(new TopologyChangeListener(this));
      keyProducerStartLatch.open();
      started = true;
   }

   @Override
   public void stop() {
   }

   public void handleViewChange(ViewChangedEvent vce) {
      if (log.isTraceEnabled()) {
         log.trace("ViewChange received: " + vce);
      }
      synchronized (address2key) {
         for (Address address : vce.getOldMembers()) {
            BlockingQueue queue = address2key.remove(address);
            if (queue == null) {
               KeyAffinityServiceImpl.log.warn("Null queue not expected for address: " + address + ". Did we miss a view change?");
            }
         }
         addQueuesForAddresses(vce.getNewMembers());
      }
   }


   public class KeyGeneratorWorker implements Runnable {

      @Override
      public void run() {
         while (true) {
            try {
               keyProducerStartLatch.await();
            } catch (InterruptedException e) {
               if (log.isInfoEnabled()) {
                  log.info("Shutting down KeyAffinity service for key set: " + filter);
               }
               return;
            }
            generateKeys();
         }
      }

      private void generateKeys() {
         maxNumberInvariant.writeLock().lock();
         try {
            for (Address address : address2key.keySet()) {
               Object key = keyGenerator.getKey();
               tryAddKey(address, key);
               if (maxNumberOfKeys.equals(exitingNumberOfKeys)) {
                  keyProducerStartLatch.close();
                  return;
               }
            }
         } finally {
            maxNumberInvariant.writeLock().unlock();
         }
      }

      private void tryAddKey(Address address, Object key) {
         BlockingQueue queue = address2key.get(address);
         boolean added = queue.offer(key);
         if (log.isTraceEnabled()) {
            log.trace((added ? "Successfully " : "Not") + "added key(" + key + ") to the address(" + address + ").");
         }
      }
   }

   /**
    * Important: this *MUST* be called with WL on {@link #address2key}.
    */
   private void revisitNumberOfKeys() {
      maxNumberOfKeys.set(address2key.keySet().size() * bufferSize);
      for (Address address : address2key.keySet()) {
         exitingNumberOfKeys.addAndGet(address2key.get(address).size());
      }
      if (log.isTraceEnabled()) {
         log.trace("revisitNumberOfKeys ends with: maxNumberOfKeys=" + maxNumberOfKeys +
               ", exitingNumberOfKeys=" + exitingNumberOfKeys);
      }
   }

   /**
    * Important: this *MUST* be called with WL on {@link #address2key}.
    */
   private void addQueuesForAddresses(Collection<Address> addresses) {
      for (Address address : addresses) {
         if (interestedInAddress(address)) {
            address2key.put(address, new ArrayBlockingQueue(bufferSize));
         } else {
            if (log.isTraceEnabled())
               log.trace("Skipping address: " + address);
         }
      }
   }

   private boolean interestedInAddress(Address address) {
      return filter == null || filter.contains(address);
   }

   private List<Address> getExistingNodes() {
      return cache.getAdvancedCache().getRpcManager().getTransport().getMembers();
   }

   private Address getAddressForKey(Object key) {
      DistributionManager distributionManager = getDistributionManager();
      ConsistentHash hash = distributionManager.getConsistentHash();
      List<Address> addressList = hash.locate(key, 1);
      if (addressList.size() == 0) {
         throw new IllegalStateException("Empty address list returned by consistent hash " + hash + " for key " + key);
      }
      return addressList.get(0);
   }

   private DistributionManager getDistributionManager() {
      DistributionManager distributionManager = cache.getAdvancedCache().getDistributionManager();
      if (distributionManager == null) {
         throw new IllegalStateException("Null distribution manager. Is this an distributed(v.s. replicated) cache?");
      }
      return distributionManager;
   }

   public Map<Address, BlockingQueue> getAddress2KeysMapping() {
      return Collections.unmodifiableMap(address2key);
   }
}
