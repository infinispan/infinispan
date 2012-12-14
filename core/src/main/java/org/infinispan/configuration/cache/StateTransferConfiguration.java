/*
 * Copyright 2011 Red Hat, Inc. and/or its affiliates.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA
 */
package org.infinispan.configuration.cache;

/**
 * Configures how state is retrieved when a new cache joins the cluster.
 * Used with invalidation and replication clustered modes.
 * 
 * @since 5.1
 */
public class StateTransferConfiguration {

   private boolean fetchInMemoryState;
   private Boolean originalFetchInMemoryState;
   private long timeout;
   private int chunkSize;
   private boolean waitForInitialStateTransferToComplete;
   private Boolean originalWaitForInitialStateTransferToComplete;

   StateTransferConfiguration(boolean fetchInMemoryState, Boolean originalFetchInMemoryState, long timeout, int chunkSize,
                              boolean waitForInitialStateTransferToComplete, Boolean originalWaitForInitialStateTransferToComplete) {
      this.fetchInMemoryState = fetchInMemoryState;
      this.originalFetchInMemoryState = originalFetchInMemoryState;
      this.timeout = timeout;
      this.chunkSize = chunkSize;
      this.waitForInitialStateTransferToComplete = waitForInitialStateTransferToComplete;
      this.originalWaitForInitialStateTransferToComplete = originalWaitForInitialStateTransferToComplete;
   }

   /**
    * If {@code true}, the cache will fetch data from the neighboring caches when it starts up, so
    * the cache starts 'warm', although it will impact startup time.
    * <p/>
    * In distributed mode, state is transferred between running caches as well, as the ownership of
    * keys changes (e.g. because a cache left the cluster). Disabling this setting means a key will
    * sometimes have less than {@code numOwner} owners.
    */
   public boolean fetchInMemoryState() {
      return fetchInMemoryState;
   }

   /**
    * We want to remember if the user didn't configure fetchInMemoryState for the default cache.
    */
   protected Boolean originalFetchInMemoryState() {
      return originalFetchInMemoryState;
   }

   /**
    * This is the maximum amount of time - in milliseconds - to wait for state from neighboring
    * caches, before throwing an exception and aborting startup.
    */
   public long timeout() {
      return timeout;
   }

   /**
    * This is the maximum amount of time - in milliseconds - to wait for state from neighboring
    * caches, before throwing an exception and aborting startup.
    */
   public StateTransferConfiguration timeout(long l) {
      timeout = l;
      return this;
   }

   /**
    * If &gt; 0, the state will be transferred in batches of {@code chunkSize} cache entries.
    * If &lt;= 0, the state will be transferred in all at once. Not recommended.
    */
   public int chunkSize() {
      return chunkSize;
   }

   /**
    * If {@code true}, the {@code CacheManager.getCache()} call will not return until state transfer is complete for
    * this cache on the current node, ie. its {@code DataContainer} has finished receiving the entries it should hold
    * according to the new {@code ConsistentHash}. This option is only available in clustered mode and the default value is {@code true}.
    */
   public boolean waitForInitialStateTransferToComplete() {
      return waitForInitialStateTransferToComplete;
   }

   /**
    * We want to remember if the user didn't configure waitForInitialStateTransferToComplete for the default cache.
    */
   protected Boolean originalWaitForInitialStateTransferToComplete() {
      return originalWaitForInitialStateTransferToComplete;
   }

   @Override
   public String toString() {
      return "StateTransferConfiguration{" +
            "chunkSize=" + chunkSize +
            ", fetchInMemoryState=" + fetchInMemoryState +
            ", originalFetchInMemoryState=" + originalFetchInMemoryState +
            ", timeout=" + timeout +
            ", waitForInitialStateTransferToComplete=" + waitForInitialStateTransferToComplete +
            ", originalWaitForInitialStateTransferToComplete=" + originalWaitForInitialStateTransferToComplete +
            '}';
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      StateTransferConfiguration that = (StateTransferConfiguration) o;

      if (chunkSize != that.chunkSize) return false;
      if (fetchInMemoryState != that.fetchInMemoryState) return false;
      if (timeout != that.timeout) return false;
      if (originalFetchInMemoryState != null ? !originalFetchInMemoryState.equals(that.originalFetchInMemoryState) : that.originalFetchInMemoryState != null)
         return false;
      if (waitForInitialStateTransferToComplete != that.waitForInitialStateTransferToComplete) return false;
      if (originalWaitForInitialStateTransferToComplete != null ? !originalWaitForInitialStateTransferToComplete.equals(that.originalWaitForInitialStateTransferToComplete) : that.originalWaitForInitialStateTransferToComplete != null)
         return false;

      return true;
   }

   @Override
   public int hashCode() {
      int result = (fetchInMemoryState ? 1 : 0);
      result = 31 * result + (originalFetchInMemoryState != null ? originalFetchInMemoryState.hashCode() : 0);
      result = 31 * result + (int) (timeout ^ (timeout >>> 32));
      result = 31 * result + chunkSize;
      result = 31 * result + (waitForInitialStateTransferToComplete ? 1 : 0);
      result = 31 * result + (originalWaitForInitialStateTransferToComplete != null ? originalWaitForInitialStateTransferToComplete.hashCode() : 0);
      return result;
   }

}
