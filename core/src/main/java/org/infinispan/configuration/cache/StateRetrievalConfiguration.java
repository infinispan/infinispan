/*
 * JBoss, Home of Professional Open Source
 * Copyright 2011 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License, v. 2.1.
 * This program is distributed in the hope that it will be useful, but WITHOUT A
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License,
 * v.2.1 along with this distribution; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA  02110-1301, USA.
 */

package org.infinispan.configuration.cache;

/**
 * Configures how state is retrieved when a new cache joins the cluster.
 * Used with invalidation and replication clustered modes.
 *
 * @deprecated Use {@link StateTransferConfiguration} instead.
 */
public class StateRetrievalConfiguration {

   private final StateTransferConfiguration stateTransferConfiguration;

   StateRetrievalConfiguration(StateTransferConfiguration stateTransferConfiguration) {
      this.stateTransferConfiguration = stateTransferConfiguration;
   }

   /**
    * If true, this will allow the cache to provide in-memory state to a neighbor, even if the cache
    * is not configured to fetch state from its neighbors (fetchInMemoryState is false)
    * @deprecated No longer used
    */
   public boolean alwaysProvideInMemoryState() {
      return false;
   }

   /**
    * If true, this will cause the cache to ask neighboring caches for state when it starts up, so
    * the cache starts 'warm', although it will impact startup time.
    * @deprecated Use {@link org.infinispan.configuration.cache.StateTransferConfiguration#fetchInMemoryState()} instead.
    */
   public boolean fetchInMemoryState() {
      return stateTransferConfiguration.fetchInMemoryState();
   }
   
   /**
    * If true, this will cause the cache to ask neighboring caches for state when it starts up, so
    * the cache starts 'warm', although it will impact startup time.
    * @deprecated No longer used, {@link org.infinispan.configuration.cache.StateTransferConfiguration#fetchInMemoryState()} is not dynamic.
    */
   public StateRetrievalConfiguration fetchInMemoryState(boolean b) {
      throw new UnsupportedOperationException("fetchInMemoryState cannot be modified after startup");
   }

   /**
    * @deprecated No longer used
    */
   protected Boolean originalFetchInMemoryState() {
      return null;
   }
   

   /**
    * Initial wait time when backing off before retrying state transfer retrieval
    * @deprecated No longer used
    */
   public long initialRetryWaitTime() {
      return 0;
   }

   /**
    * This is the maximum amount of time to run a cluster-wide flush, to allow for syncing of
    * transaction logs.
    * @deprecated No longer used
    */
   public long logFlushTimeout() {
      return 0;
   }

   /**
    * This is the maximum number of non-progressing transaction log writes after which a
    * brute-force flush approach is resorted to, to synchronize transaction logs.
    * @deprecated No longer used
    */
   public int maxNonProgressingLogWrites() {
      return 0;
   }

   /**
    * Number of state retrieval retries before giving up and aborting startup.
    * @deprecated No longer used
    */
   public int numRetries() {
      return 0;
   }

   /**
    * Wait time increase factor over successive state retrieval backoffs
    * @deprecated No longer used
    */
   public int retryWaitTimeIncreaseFactor() {
      return 0;
   }

   /**
    * This is the maximum amount of time - in milliseconds - to wait for state from neighboring
    * caches, before throwing an exception and aborting startup.
    * @deprecated Use {@link org.infinispan.configuration.cache.StateTransferConfiguration#timeout()} instead.
    */
   public long timeout() {
      return stateTransferConfiguration.timeout();
   }
   
   /**
    * This is the maximum amount of time - in milliseconds - to wait for state from neighboring
    * caches, before throwing an exception and aborting startup.
    * @deprecated Use {@link org.infinispan.configuration.cache.StateTransferConfiguration#timeout(long)} instead.
    */
   public StateRetrievalConfiguration timeout(long l) {
      stateTransferConfiguration.timeout(l);
      return this;
   }

}
