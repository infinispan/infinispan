package org.infinispan.persistence;

import org.infinispan.container.InternalEntryFactory;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.InternalCacheValue;
import org.infinispan.marshall.core.MarshalledEntry;
import org.infinispan.metadata.InternalMetadata;
import org.infinispan.metadata.InternalMetadataImpl;
import org.infinispan.persistence.spi.AdvancedCacheLoader;
import org.infinispan.util.concurrent.WithinThreadExecutor;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Mircea Markus
 * @since 6.0
 */
public class PersistenceUtil {

   private static Log log = LogFactory.getLog(PersistenceUtil.class);

   public static AdvancedCacheLoader.KeyFilter notNull(AdvancedCacheLoader.KeyFilter filter) {
      return filter == null ? AdvancedCacheLoader.KeyFilter.LOAD_ALL_FILTER : filter;
   }

   public static int count(AdvancedCacheLoader acl, AdvancedCacheLoader.KeyFilter filter) {
      final AtomicInteger result = new AtomicInteger(0);
      acl.process(filter, new AdvancedCacheLoader.CacheLoaderTask() {
         @Override
         public void processEntry(MarshalledEntry marshalledEntry, AdvancedCacheLoader.TaskContext taskContext) throws InterruptedException {
            result.incrementAndGet();
         }
      }, new WithinThreadExecutor(), false, false);
      return result.get();
   }

   public static Set<Object> toKeySet(AdvancedCacheLoader acl, AdvancedCacheLoader.KeyFilter filter) {
      if (acl == null)
         return Collections.emptySet();
      final Set<Object> set = new HashSet<Object>();
      acl.process(filter, new AdvancedCacheLoader.CacheLoaderTask() {
         @Override
         public void processEntry(MarshalledEntry marshalledEntry, AdvancedCacheLoader.TaskContext taskContext) throws InterruptedException {
            set.add(marshalledEntry.getKey());
         }
      }, new WithinThreadExecutor(), false, false);
      return set;
   }

   public static Set<InternalCacheEntry> toEntrySet(AdvancedCacheLoader acl, AdvancedCacheLoader.KeyFilter filter, final InternalEntryFactory ief) {
      if (acl == null)
         return Collections.emptySet();
      final Set<InternalCacheEntry> set = new HashSet<InternalCacheEntry>();
      acl.process(filter, new AdvancedCacheLoader.CacheLoaderTask() {
         @Override
         public void processEntry(MarshalledEntry ce, AdvancedCacheLoader.TaskContext taskContext) throws InterruptedException {
            set.add(ief.create(ce.getKey(), ce.getValue(), ce.getMetadata()));
         }
      }, new WithinThreadExecutor(), true, true);
      return set;
   }

   public static long getExpiryTime(InternalMetadata internalMetadata) {
      return internalMetadata == null ? -1 : internalMetadata.expiryTime();
   }

   public static InternalMetadata internalMetadata(InternalCacheEntry ice) {
      return ice.getMetadata() == null ? null : new InternalMetadataImpl(ice);
   }

   public static InternalMetadata internalMetadata(InternalCacheValue icv) {
      return icv.getMetadata() == null ? null : new InternalMetadataImpl(icv.getMetadata(), icv.getCreated(), icv.getLastUsed());
   }
}
