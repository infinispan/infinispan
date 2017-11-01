package org.jboss.as.clustering.infinispan.conflict;

import java.util.List;

import org.infinispan.conflict.EntryMergePolicy;
import org.infinispan.container.entries.CacheEntry;

public class DeployedMergePolicy implements EntryMergePolicy {

   private final String className;
   private EntryMergePolicy delegate;

   public DeployedMergePolicy(String className) {
      this.className = className;
   }

   public DeployedMergePolicy(EntryMergePolicy delegate) {
      this.className = delegate.getClass().getName();
      this.delegate = delegate;
   }

   public String getClassName() {
      return className;
   }

   public EntryMergePolicy getDelegate() {
      return delegate;
   }

   public void setDelegate(EntryMergePolicy delegate) {
      this.delegate = delegate;
   }

   @Override
   public CacheEntry merge(CacheEntry preferredEntry, List otherEntries) {
      return delegate.merge(preferredEntry, otherEntries);
   }
}
