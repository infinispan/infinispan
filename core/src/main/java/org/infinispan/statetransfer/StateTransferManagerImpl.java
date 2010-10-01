/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2000 - 2008, Red Hat Middleware LLC, and individual contributors
 * as indicated by the @author tags. See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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
import org.infinispan.commands.control.StateTransferControlCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.config.Configuration;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.InvocationContextContainer;
import static org.infinispan.context.Flag.CACHE_MODE_LOCAL;
import org.infinispan.context.impl.RemoteTxInvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.interceptors.InterceptorChain;
import org.infinispan.io.UnclosableObjectInputStream;
import org.infinispan.io.UnclosableObjectOutputStream;
import org.infinispan.loaders.CacheLoaderException;
import org.infinispan.loaders.CacheLoaderManager;
import org.infinispan.loaders.CacheStore;
import org.infinispan.marshall.StreamingMarshaller;
import org.infinispan.remoting.rpc.ResponseMode;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.DistributedSync;
import org.infinispan.transaction.TransactionLog;
import org.infinispan.transaction.xa.RemoteTransaction;
import org.infinispan.transaction.xa.TransactionTable;
import org.infinispan.util.Util;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

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
   volatile boolean needToUnblockRPC = false;
   volatile Address stateSender;

   @Inject
   @SuppressWarnings("unchecked")
   public void injectDependencies(RpcManager rpcManager, AdvancedCache cache, Configuration configuration,
                                  DataContainer dataContainer, CacheLoaderManager clm, StreamingMarshaller marshaller,
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
      log.trace("Data container is {0}", System.identityHashCode(dataContainer));
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
            log.debug("State transfer process completed in {0}", Util.prettyPrintTime(duration));
         }
      }
   }

   @Start(priority = 1000)
   // needs to be the last thing that happens on this cache
   public void releaseRPCBlock() throws Exception {
      if (needToUnblockRPC) {
         if (trace) log.trace("Stopping RPC block");
         mimicPartialFlushViaRPC(stateSender, false);
      }
   }

   public void generateState(OutputStream out) throws StateTransferException {
      ObjectOutput oo = null;
      boolean txLogActivated = false;
      try {
         boolean canProvideState = (txLogActivated = transactionLog.activate());
         if (log.isDebugEnabled()) log.debug("Generating state.  Can provide? {0}", canProvideState);
         oo = marshaller.startObjectOutput(out, false);

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
         if (trace) log.trace("Transaction log size is {0}", transactionLog.size());
         for (int nonProgress = 0, size = transactionLog.size(); size > 0;) {
            if (trace) log.trace("Tx Log remaining entries = " + size);
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
         distributedSync.releaseProcessingLock();
      }
   }

   private void processCommitLog(ObjectInput oi) throws Exception {
      if (trace) log.trace("Applying commit log");
      Object object = marshaller.objectFromObjectStream(oi);
      while (object instanceof TransactionLog.LogEntry) {
         TransactionLog.LogEntry logEntry = (TransactionLog.LogEntry) object;
         InvocationContext ctx = invocationContextContainer.createRemoteInvocationContext();
         WriteCommand[] mods = logEntry.getModifications();
         if (trace) log.trace("Mods = {0}", Arrays.toString(mods));
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
      mimicPartialFlushViaRPC(stateSender, true);
      needToUnblockRPC = true;

      try {
         if (trace)
            log.trace("Retrieving/Applying post-flush commits");
         processCommitLog(oi);

         if (trace)
            log.trace("Retrieving/Applying pending prepares");
         Object object = marshaller.objectFromObjectStream(oi);
         while (object instanceof PrepareCommand) {
            PrepareCommand command = (PrepareCommand) object;

            if (!transactionLog.hasPendingPrepare(command)) {
               if (trace) log.trace("Applying pending prepare {0}", command);
               commandsFactory.initializeReplicableCommand(command, false);
               RemoteTxInvocationContext ctx = invocationContextContainer.createRemoteTxInvocationContext();
               RemoteTransaction transaction = txTable.createRemoteTransaction(command.getGlobalTransaction(), command.getModifications());
               ctx.setRemoteTransaction(transaction);
               ctx.setFlags(CACHE_MODE_LOCAL, Flag.SKIP_CACHE_STATUS_CHECK);
               interceptorChain.invoke(ctx, command);
            } else {
               if (trace) log.trace("Prepare {0} not in tx log; not applying", command);
            }
            object = marshaller.objectFromObjectStream(oi);
         }
         assertDelimited(object);
      } catch (Exception e) {
         if (trace) log.trace("Stopping RPC block");
         mimicPartialFlushViaRPC(stateSender, false);
         needToUnblockRPC = false;
         throw e;
      }
   }

   /**
    * Mimics a partial flush between the current instance and the address to flush, by opening and closing the necessary
    * latches on both ends.
    *
    * @param addressToFlush address to flush in addition to the current address
    * @param block          if true, mimics setting a flush.  Otherwise, mimics un-setting a flush.
    * @throws Exception if there are issues
    */
   private void mimicPartialFlushViaRPC(Address addressToFlush, boolean block) throws Exception {
      StateTransferControlCommand cmd = commandsFactory.buildStateTransferControlCommand(block);
      if (!block) rpcManager.getTransport().getDistributedSync().releaseSync();
      rpcManager.invokeRemotely(Collections.singletonList(addressToFlush), cmd, ResponseMode.SYNCHRONOUS, configuration.getStateRetrievalTimeout(), true);
      if (block) rpcManager.getTransport().getDistributedSync().acquireSync();
   }

   public void applyState(InputStream in) throws StateTransferException {
      if (log.isDebugEnabled()) log.debug("Applying state");
      ObjectInput oi = null;
      try {
         oi = marshaller.startObjectInput(in, false);
         // Started flag controls whether remote cache was started and hence provided state
         boolean started = (Boolean) marshaller.objectFromObjectStream(oi);
         if (started) {
            boolean canProvideState = (Boolean) marshaller.objectFromObjectStream(oi);
            if (canProvideState) {
               assertDelimited(oi);
               // First clear the cache store!!
               if (cs != null) cs.clear();
               if (transientState) applyInMemoryState(oi);
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

   private void applyInMemoryState(ObjectInput i) throws StateTransferException {
      dataContainer.clear();
      try {
         Set<InternalCacheEntry> set = (Set<InternalCacheEntry>) marshaller.objectFromObjectStream(i);
         for (InternalCacheEntry se : set)
            cache.withFlags(CACHE_MODE_LOCAL).put(se.getKey(), se.getValue(), se.getLifespan(), MILLISECONDS, se.getMaxIdle(), MILLISECONDS);
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
         if (log.isDebugEnabled()) log.debug("Writing {0} StoredEntries to stream", entries.size());
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
