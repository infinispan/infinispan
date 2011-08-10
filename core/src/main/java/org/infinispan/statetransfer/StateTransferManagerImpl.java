/*
 * JBoss, Home of Professional Open Source
 * Copyright 2009 Red Hat Inc. and/or its affiliates and other
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
package org.infinispan.statetransfer;

import org.infinispan.AdvancedCache;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.config.Configuration;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.InvocationContextContainer;
import org.infinispan.context.impl.RemoteTxInvocationContext;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.interceptors.InterceptorChain;
import org.infinispan.io.UnclosableObjectInputStream;
import org.infinispan.io.UnclosableObjectOutputStream;
import org.infinispan.loaders.CacheLoaderException;
import org.infinispan.loaders.CacheLoaderManager;
import org.infinispan.loaders.CacheStore;
import org.infinispan.marshall.StreamingMarshaller;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.DistributedSync;
import org.infinispan.transaction.RemoteTransaction;
import org.infinispan.transaction.TransactionLog;
import org.infinispan.transaction.TransactionTable;
import org.infinispan.util.Util;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.io.*;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.infinispan.context.Flag.CACHE_MODE_LOCAL;
import static org.infinispan.context.Flag.SKIP_CACHE_STORE;
import static org.infinispan.context.Flag.SKIP_SHARED_CACHE_STORE;
import static org.infinispan.factories.KnownComponentNames.CACHE_MARSHALLER;

public class StateTransferManagerImpl implements StateTransferManager {

   RpcManager rpcManager;
   AdvancedCache<Object, Object> cache;
   Configuration configuration;
   DataContainer dataContainer;
   CacheLoaderManager clm;
   CacheStore cs;
   StreamingMarshaller marshaller;
   TransactionLog transactionLog;
   InvocationContextContainer invocationContextContainer;
   InterceptorChain interceptorChain;
   CommandsFactory commandsFactory;
   TransactionTable txTable;
   private static final Log log = LogFactory.getLog(StateTransferManagerImpl.class);
   private static final boolean trace = log.isTraceEnabled();
   private static final Byte DELIMITER = (byte) 123;

   boolean transientState, persistentState, alwaysProvideTransientState;
   int maxNonProgressingLogWrites;
   long flushTimeout;
   volatile Address stateSender;

   @Inject
   @SuppressWarnings("unchecked")
   public void injectDependencies(RpcManager rpcManager, AdvancedCache cache, Configuration configuration,
                                  DataContainer dataContainer, CacheLoaderManager clm, @ComponentName(CACHE_MARSHALLER) StreamingMarshaller marshaller,
                                  TransactionLog transactionLog, InterceptorChain interceptorChain, InvocationContextContainer invocationContextContainer,
                                  CommandsFactory commandsFactory, TransactionTable txTable) {
      this.rpcManager = rpcManager;
      this.cache = cache;
      this.configuration = configuration;
      this.dataContainer = dataContainer;
      this.clm = clm;
      this.marshaller = marshaller;
      this.transactionLog = transactionLog;
      this.invocationContextContainer = invocationContextContainer;
      this.interceptorChain = interceptorChain;
      this.commandsFactory = commandsFactory;
      this.txTable = txTable;
   }

   @Start(priority = 55)
   // it is imperative that this starts *after* the RpcManager does, and *after* the cache loader manager (if any) inits and preloads
   public void start() throws StateTransferException {
      log.tracef("Data container is %s", Util.hexIdHashCode(dataContainer));
      cs = clm == null ? null : clm.getCacheStore();
      transientState = configuration.isFetchInMemoryState();
      alwaysProvideTransientState = configuration.isAlwaysProvideInMemoryState();
      persistentState = cs != null && clm.isEnabled() && clm.isFetchPersistentState() && !clm.isShared();
      maxNonProgressingLogWrites = configuration.getStateRetrievalMaxNonProgressingLogWrites();
      flushTimeout = configuration.getStateRetrievalLogFlushTimeout();

      if (transientState || persistentState) {
         long startTime = 0;
            if (log.isDebugEnabled()) {
            log.debug("Initiating state transfer process");
            startTime = System.currentTimeMillis();
         }

         rpcManager.retrieveState(cache.getName(), configuration.getStateRetrievalTimeout());

         if (log.isDebugEnabled()) {
            long duration = System.currentTimeMillis() - startTime;
            log.debugf("State transfer process completed in %s", Util.prettyPrintTime(duration));
         }
      }
   }

   public void generateState(OutputStream out) throws StateTransferException {
      ObjectOutput oo = null;
      boolean txLogActivated = false;
      try {
         boolean canProvideState = (txLogActivated = transactionLog.activate());
         if (log.isDebugEnabled()) log.debugf("Generating state.  Can provide? %s", canProvideState);
         oo = marshaller.startObjectOutput(out, true);

         // If we can generate state, we've started up 
         marshaller.objectToObjectStream(true, oo);
         marshaller.objectToObjectStream(canProvideState, oo);

         if (canProvideState) {
            delimit(oo);
            if (transientState || alwaysProvideTransientState) generateInMemoryState(oo); // always provide in-memory state if requested.  ISPN-610.
            delimit(oo);
            if (persistentState) generatePersistentState(oo);
            delimit(oo);
            generateTransactionLog(oo);

            if (log.isDebugEnabled()) log.debug("State generated, closing object stream");
         } else {
            if (log.isDebugEnabled()) log.debug("Not providing state!");
         }

      } catch (StateTransferException ste) {
         throw ste;
      } catch (Exception e) {
         throw new StateTransferException(e);
      } finally {
         marshaller.finishObjectOutput(oo);
         if (txLogActivated) transactionLog.deactivate();
      }
   }

   private void generateTransactionLog(ObjectOutput oo) throws Exception {
      DistributedSync distributedSync = rpcManager.getTransport().getDistributedSync();

      try {
         if (trace) log.tracef("Transaction log size is %s", transactionLog.size());
         for (int nonProgress = 0, size = transactionLog.size(); size > 0;) {
            if (trace) log.tracef("Tx Log remaining entries = %d", size);
            transactionLog.writeCommitLog(marshaller, oo);
            int newSize = transactionLog.size();

            // If size did not decrease then we did not make progress, and could be wasting
            // our time. Limit this to the specified max.
            if (newSize >= size && ++nonProgress >= maxNonProgressingLogWrites)
               break;

            size = newSize;
         }

         // Wait on incoming and outgoing threads to line-up in front of
         // the distributed sync.
         distributedSync.acquireProcessingLock(true, configuration.getStateRetrievalTimeout(), MILLISECONDS);

         // Signal to sender that we need a flush to get a consistent view
         // of the remaining transactions.
         delimit(oo);
         oo.flush();
         if (trace) log.trace("Waiting for a distributed sync block");
         distributedSync.blockUntilAcquired(flushTimeout, MILLISECONDS);

         if (trace) log.trace("Distributed sync block received, proceeding with writing commit log");
         // Write remaining transactions
         transactionLog.writeCommitLog(marshaller, oo);
         delimit(oo);

         // Write all non-completed prepares
         transactionLog.writePendingPrepares(marshaller, oo);
         delimit(oo);
         oo.flush();
      }
      finally {
         distributedSync.releaseProcessingLock(true);
      }
   }

   private void processCommitLog(ObjectInput oi) throws Exception {
      if (trace) log.trace("Applying commit log");
      Object object = marshaller.objectFromObjectStream(oi);
      while (object instanceof TransactionLog.LogEntry) {
         TransactionLog.LogEntry logEntry = (TransactionLog.LogEntry) object;
         InvocationContext ctx = invocationContextContainer.createRemoteInvocationContext(null /* No idea if this right PLM */);
         WriteCommand[] mods = logEntry.getModifications();
         if (trace) log.tracef("Mods = %s", Arrays.toString(mods));
         for (WriteCommand mod : mods) {
            commandsFactory.initializeReplicableCommand(mod, false);
            ctx.setFlags(CACHE_MODE_LOCAL, Flag.SKIP_CACHE_STATUS_CHECK);
            interceptorChain.invoke(ctx, mod);
         }

         object = marshaller.objectFromObjectStream(oi);
      }

      assertDelimited(object);
      if (trace) log.trace("Finished applying commit log");
   }

   private void applyTransactionLog(ObjectInput oi) throws Exception {
      if (trace) log.trace("Integrating transaction log");

      processCommitLog(oi);
      stateSender = rpcManager.getCurrentStateTransferSource();
      
      if (trace)
         log.trace("Retrieving/Applying post-flush commits");
      processCommitLog(oi);

      if (trace)
         log.trace("Retrieving/Applying pending prepares");
      Object object = marshaller.objectFromObjectStream(oi);
      while (object instanceof PrepareCommand) {
         PrepareCommand command = (PrepareCommand) object;

         if (!transactionLog.hasPendingPrepare(command)) {
            if (trace) log.tracef("Applying pending prepare %s", command);
            commandsFactory.initializeReplicableCommand(command, false);
            RemoteTxInvocationContext ctx = invocationContextContainer.createRemoteTxInvocationContext(null /* No idea if this right PLM */);
            RemoteTransaction transaction = txTable.createRemoteTransaction(command.getGlobalTransaction(), command.getModifications());
            ctx.setRemoteTransaction(transaction);
            ctx.setFlags(CACHE_MODE_LOCAL, Flag.SKIP_CACHE_STATUS_CHECK);
            interceptorChain.invoke(ctx, command);
         } else {
            if (trace) log.tracef("Prepare %s not in tx log; not applying", command);
         }
         object = marshaller.objectFromObjectStream(oi);
      }
      assertDelimited(object);      
   }
   
   public void applyState(InputStream in) throws StateTransferException {
      if (log.isDebugEnabled()) log.debug("Applying state");
      ObjectInput oi = null;
      try {
         oi = marshaller.startObjectInput(in, true);
         // Started flag controls whether remote cache was started and hence provided state
         boolean started = (Boolean) marshaller.objectFromObjectStream(oi);
         if (started) {
            boolean canProvideState = (Boolean) marshaller.objectFromObjectStream(oi);
            if (canProvideState) {
               assertDelimited(oi);
               if (transientState) applyInMemoryState(oi, persistentState && !configuration.isCacheLoaderPassivation());
               assertDelimited(oi);
               if (persistentState) applyPersistentState(oi);
               assertDelimited(oi);
               applyTransactionLog(oi);
               if (log.isDebugEnabled()) log.debug("State applied, closing object stream");
            } else {
               String msg = "Provider cannot provide state!";
               if (log.isDebugEnabled()) log.debug(msg);
               throw new StateTransferException(msg);
            }
         }
      } catch (StateTransferException ste) {
         throw ste;
      } catch (Exception e) {
         throw new StateTransferException(e);
      } finally {
         // just close the object stream but do NOT close the underlying stream
         marshaller.finishObjectInput(oi);
      }
   }

   @SuppressWarnings("unchecked")
   private void applyInMemoryState(ObjectInput i, boolean skipCacheStore) throws StateTransferException {
      dataContainer.clear();
      // if the persistent state is transferred separately, we don't want to write to the cache store here
      // if not, we want to write to the cache store unless it's shared
      Flag[] flags = skipCacheStore
            ? new Flag[]{ CACHE_MODE_LOCAL, SKIP_CACHE_STORE }
            : new Flag[]{ CACHE_MODE_LOCAL, SKIP_SHARED_CACHE_STORE};
      try {
         Set<InternalCacheEntry> set = (Set<InternalCacheEntry>) marshaller.objectFromObjectStream(i);
         for (InternalCacheEntry se : set)
            cache.withFlags(flags).put(se.getKey(), se.getValue(), se.getLifespan(), MILLISECONDS, se.getMaxIdle(), MILLISECONDS);
      } catch (Exception e) {
         dataContainer.clear();
         throw new StateTransferException(e);
      }
   }

   private void generateInMemoryState(ObjectOutput oo) throws StateTransferException {
      // write all StoredEntries to the stream using the marshaller.
      // TODO is it safe enough to get these from the data container directly?
      try {
         Set<InternalCacheEntry> entries = new HashSet<InternalCacheEntry>();
         for (InternalCacheEntry e : dataContainer) {
            if (!e.isExpired()) entries.add(e);
         }
         if (log.isDebugEnabled()) log.debugf("Writing %s StoredEntries to stream", entries.size());
         marshaller.objectToObjectStream(entries, oo);
      } catch (Exception e) {
         throw new StateTransferException(e);
      }
   }

   private void applyPersistentState(ObjectInput i) throws StateTransferException {
      try {
         // always use the unclosable stream delegate to ensure the impl doesn't close the stream
         cs.fromStream(new UnclosableObjectInputStream(i));
      } catch (CacheLoaderException cle) {
         throw new StateTransferException(cle);
      }
   }

   private void generatePersistentState(ObjectOutput oo) throws StateTransferException {
      try {
         // always use the unclosable stream delegate to ensure the impl doesn't close the stream
         if (trace) log.trace("Generate persistent state");
         cs.toStream(new UnclosableObjectOutputStream(oo));
      } catch (CacheLoaderException cle) {
         throw new StateTransferException(cle);
      }
   }

   private void delimit(ObjectOutput oo) throws IOException {
      marshaller.objectToObjectStream(DELIMITER, oo);
   }

   private void assertDelimited(ObjectInput i) throws StateTransferException {
      Object o;
      try {
         o = marshaller.objectFromObjectStream(i);
      } catch (Exception e) {
         throw new StateTransferException(e);
      }
      assertDelimited(o);
   }

   private void assertDelimited(Object o) throws StateTransferException {
      if (o instanceof Exception) throw new StateTransferException((Exception) o);
      if (!DELIMITER.equals(o)) throw new StateTransferException("Expected a delimiter, recieved " + o);
   }
}
