package org.infinispan.persistence.manager;

/**
 * @since 13.0
 */
public class PersistenceStatus {
   private final boolean isEnabled;
   private final boolean usingSegmentedStore;
   private final boolean usingAsyncStore;
   private final boolean usingSharedAsyncStore;
   private final boolean usingReadOnly;
   private final boolean usingTransactionalStore;

   public PersistenceStatus(boolean isEnabled, boolean usingSegmentedStore, boolean usingAsyncStore,
         boolean usingSharedAsyncStore, boolean usingReadOnly, boolean usingTransactionalStore) {
      this.isEnabled = isEnabled;
      this.usingSegmentedStore = usingSegmentedStore;
      this.usingAsyncStore = usingAsyncStore;
      this.usingSharedAsyncStore = usingSharedAsyncStore;
      this.usingReadOnly = usingReadOnly;
      this.usingTransactionalStore = usingTransactionalStore;
   }

   public boolean isEnabled() {
      return isEnabled;
   }

   public boolean usingSegmentedStore() {
      return usingSegmentedStore;
   }

   public boolean usingAsyncStore() {
      return usingAsyncStore;
   }

   public boolean usingSharedAsyncStore() {
      return usingSharedAsyncStore;
   }

   public boolean usingReadOnly() {
      return usingReadOnly;
   }

   public boolean usingTransactionalStore() {
      return usingTransactionalStore;
   }
}
