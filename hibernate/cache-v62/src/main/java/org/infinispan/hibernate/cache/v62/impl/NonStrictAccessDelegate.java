package org.infinispan.hibernate.cache.v62.impl;

import java.util.Comparator;

import org.hibernate.cache.CacheException;
import org.hibernate.cache.spi.access.SoftLock;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.infinispan.hibernate.cache.commons.InfinispanDataRegion;
import org.infinispan.hibernate.cache.commons.util.VersionedEntry;

/**
 * Access delegate that relaxes the consistency a bit: stale reads are prohibited only after the transaction
 * commits. This should also be able to work with async caches, and that would allow the replication delay
 * even after the commit.
 *
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
public class NonStrictAccessDelegate extends org.infinispan.hibernate.cache.commons.access.NonStrictAccessDelegate {
   public NonStrictAccessDelegate(InfinispanDataRegion region, Comparator versionComparator) {
      super(region, versionComparator);
   }

   @Override
   public void remove(Object session, Object key) throws CacheException {
      Sync sync = (Sync) ((SharedSessionContractImplementor) session).getCacheTransactionSynchronization();
      sync.registerAfterCommit(new RemovalInvocation(writeMap, region, key));
   }

   @Override
   public boolean insert(Object session, Object key, Object value, Object version) throws CacheException {
      // in order to leverage asynchronous execution we won't execute the updates in afterInsert/afterUpdate
      // but register our own synchronization (exchange allocation for latency)
      Sync sync = (Sync) ((SharedSessionContractImplementor) session).getCacheTransactionSynchronization();
      sync.registerAfterCommit(new UpsertInvocation(writeMap, key, new VersionedEntry(value, version, sync.getCachingTimestamp())));
      return false;
   }

   @Override
   public boolean update(Object session, Object key, Object value, Object currentVersion, Object previousVersion) throws CacheException {
      // in order to leverage asynchronous execution we won't execute the updates in afterInsert/afterUpdate
      // but register our own synchronization (exchange allocation for latency)
      Sync sync = (Sync) ((SharedSessionContractImplementor) session).getCacheTransactionSynchronization();
      sync.registerAfterCommit(new UpsertInvocation(writeMap, key, new VersionedEntry(value, currentVersion, sync.getCachingTimestamp())));
      return false;
   }

   @Override
   public boolean afterInsert(Object session, Object key, Object value, Object version) {
      // We have not modified the cache yet but in order to update stats we need to say we did
      return true;
   }

   @Override
   public boolean afterUpdate(Object session, Object key, Object value, Object currentVersion, Object previousVersion, SoftLock lock) {
      // We have not modified the cache yet but in order to update stats we need to say we did
      return true;
   }
}
