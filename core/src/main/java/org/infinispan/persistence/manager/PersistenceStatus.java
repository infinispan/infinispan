package org.infinispan.persistence.manager;

/**
 * @since 13.0
 */
public class PersistenceStatus {
   private final boolean isEnabled;
   private final boolean usingSegmentedStore;
   private final boolean usingAsyncStore;
   private final boolean fetchPersistentState;
   private final boolean usingSharedAsyncStore;
   private final boolean usingReadOnly;

   public PersistenceStatus(boolean isEnabled, boolean usingSegmentedStore, boolean usingAsyncStore,
                            boolean fetchPersistentState, boolean usingSharedAsyncStore, boolean usingReadOnly) {
      this.isEnabled = isEnabled;
      this.usingSegmentedStore = usingSegmentedStore;
      this.usingAsyncStore = usingAsyncStore;
      this.fetchPersistentState = fetchPersistentState;
      this.usingSharedAsyncStore = usingSharedAsyncStore;
      this.usingReadOnly = usingReadOnly;
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

   public boolean fetchPersistentState() {
      return fetchPersistentState;
   }

   public boolean usingSharedAsyncStore() {
      return usingSharedAsyncStore;
   }

   public boolean usingReadOnly() {
      return usingReadOnly;
   }
}
