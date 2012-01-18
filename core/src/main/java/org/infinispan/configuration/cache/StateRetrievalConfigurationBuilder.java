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

import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.concurrent.TimeUnit;

/**
 * Configures how state is retrieved when a new cache joins the cluster. Used with invalidation and
 * replication clustered modes.
 * @deprecated Use {@link StateTransferConfigurationBuilder} instead.
 */
public class StateRetrievalConfigurationBuilder extends
      AbstractClusteringConfigurationChildBuilder<StateRetrievalConfiguration> {

   private static final Log log = LogFactory.getLog(StateRetrievalConfigurationBuilder.class);

   StateRetrievalConfigurationBuilder(ClusteringConfigurationBuilder builder) {
      super(builder);
   }

   /**
    * If true, allows the cache to provide in-memory state to a neighbor, even if the cache is not
    * configured to fetch state from its neighbors (fetchInMemoryState is false)
    * @deprecated No longer used
    */
   public StateRetrievalConfigurationBuilder alwaysProvideInMemoryState(boolean b) {
      return this;
   }

   /**
    * If true, this will cause the cache to ask neighboring caches for state when it starts up, so
    * the cache starts 'warm', although it will impact startup time.
    * @deprecated Use {@link StateTransferConfigurationBuilder#fetchInMemoryState(boolean)} instead.
    */
   public StateRetrievalConfigurationBuilder fetchInMemoryState(boolean b) {
      stateTransfer().fetchInMemoryState(b);
      return this;
   }

   /**
    * Initial wait time when backing off before retrying state transfer retrieval
    * @deprecated No longer used
    */
   public StateRetrievalConfigurationBuilder initialRetryWaitTime(long l) {
      return this;
   }

   /**
    * This is the maximum amount of time to run a cluster-wide flush, to allow for syncing of
    * transaction logs.
    * @deprecated No longer used
    */
   public StateRetrievalConfigurationBuilder logFlushTimeout(long l) {
      return this;
   }

   /**
    * This is the maximum number of non-progressing transaction log writes after which a brute-force
    * flush approach is resorted to, to synchronize transaction logs.
    * @deprecated No longer used
    */
   public StateRetrievalConfigurationBuilder maxNonProgressingLogWrites(int i) {
      return this;
   }

   /**
    * Number of state retrieval retries before giving up and aborting startup.
    * @deprecated No longer used
    */
   public StateRetrievalConfigurationBuilder numRetries(int i) {
      return this;
   }

   /**
    * Wait time increase factor over successive state retrieval backoffs
    * @deprecated No longer used
    */
   public StateRetrievalConfigurationBuilder retryWaitTimeIncreaseFactor(int i) {
      return this;
   }

   /**
    * This is the maximum amount of time - in milliseconds - to wait for state from neighboring
    * caches, before throwing an exception and aborting startup.
    * @deprecated Use {@link StateTransferConfigurationBuilder#timeout(long)} instead.
    */
   public StateRetrievalConfigurationBuilder timeout(long l) {
      stateTransfer().timeout(l);
      return this;
   }

   @Override
   void validate() {
      // do nothing
   }

   @Override
   StateRetrievalConfiguration create() {
      throw new UnsupportedOperationException("This builder is only a wrapper around StateTransferConfigurationBuilder");
   }
   
   public StateRetrievalConfigurationBuilder read(StateRetrievalConfiguration template) {
      throw new UnsupportedOperationException("This builder is only a wrapper around StateTransferConfigurationBuilder");
   }

}
