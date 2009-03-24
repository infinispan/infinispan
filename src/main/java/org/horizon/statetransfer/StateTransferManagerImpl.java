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
package org.horizon.statetransfer;

import org.horizon.AdvancedCache;
import org.horizon.commands.CommandsFactory;
import org.horizon.commands.control.StateTransferControlCommand;
import org.horizon.commands.tx.PrepareCommand;
import org.horizon.commands.write.WriteCommand;
import org.horizon.config.Configuration;
import org.horizon.container.DataContainer;
import org.horizon.container.entries.InternalCacheEntry;
import org.horizon.context.InvocationContext;
import org.horizon.factories.annotations.Inject;
import org.horizon.factories.annotations.Start;
import org.horizon.interceptors.InterceptorChain;
import org.horizon.invocation.InvocationContextContainer;
import org.horizon.invocation.Options;
import org.horizon.io.UnclosableObjectInputStream;
import org.horizon.io.UnclosableObjectOutputStream;
import org.horizon.loader.CacheLoaderException;
import org.horizon.loader.CacheLoaderManager;
import org.horizon.loader.CacheStore;
import org.horizon.logging.Log;
import org.horizon.logging.LogFactory;
import org.horizon.marshall.Marshaller;
import org.horizon.remoting.RPCManager;
import org.horizon.remoting.ResponseMode;
import org.horizon.remoting.transport.Address;
import org.horizon.remoting.transport.DistributedSync;
import org.horizon.transaction.TransactionLog;
import org.horizon.util.Util;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.Set;
import java.util.HashSet;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class StateTransferManagerImpl implements StateTransferManager {

   RPCManager rpcManager;
   AdvancedCache cache;
   Configuration configuration;
   DataContainer dataContainer;
   CacheLoaderManager clm;
   CacheStore cs;
   Marshaller marshaller;
   TransactionLog transactionLog;
   InvocationContextContainer invocationContextContainer;
   InterceptorChain interceptorChain;
   CommandsFactory commandsFactory;
   private static final Log log = LogFactory.getLog(StateTransferManagerImpl.class);
   private static final boolean trace = log.isTraceEnabled();
   private static final Byte DELIMITER = (byte) 123;

   boolean transientState, persistentState;
   volatile boolean needToUnblockRPC = false;
   volatile Address stateSender;

   @Inject
   public void injectDependencies(RPCManager rpcManager, AdvancedCache cache, Configuration configuration,
                                  DataContainer dataContainer, CacheLoaderManager clm, Marshaller marshaller,
                                  TransactionLog transactionLog, InterceptorChain interceptorChain, InvocationContextContainer invocationContextContainer,
                                  CommandsFactory commandsFactory) {
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
   }

   @Start(priority = 55)
   // it is imperative that this starts *after* the RPCManager does, and *after* the cache loader manager (if any) inits and preloads
   public void start() throws StateTransferException {
      log.trace("Data container is {0}", System.identityHashCode(dataContainer));
      cs = clm == null ? null : clm.getCacheStore();
      transientState = configuration.isFetchInMemoryState();
      persistentState = cs != null && clm.isEnabled() && clm.isFetchPersistentState() && !clm.isShared();

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

   @Start (priority = 1000) // needs to be the last thing that happens on this cache
   public void releaseRPCBlock() throws Exception {
      if (needToUnblockRPC) {
         if (trace) log.trace("Stopping RPC block");
         mimicPartialFlushViaRPC(stateSender, false);
      }
   }

   public void generateState(OutputStream out) throws StateTransferException {
      ObjectOutputStream oos = null;
      boolean txLogActivated = false;
      try {
         boolean canProvideState = (transientState || persistentState)
               && (txLogActivated = transactionLog.activate());
         if (log.isDebugEnabled()) log.debug("Generating state.  Can provide? {0}", canProvideState);
         oos = new ObjectOutputStream(out);
         marshaller.objectToObjectStream(canProvideState, oos);

         if (canProvideState) {
            delimit(oos);
            if (transientState) generateInMemoryState(oos);
            delimit(oos);
            if (persistentState) generatePersistentState(oos);
            delimit(oos);
            generateTransactionLog(oos);

            if (log.isDebugEnabled()) log.debug("State generated, closing object stream");
         } else {
            if (log.isDebugEnabled()) log.debug("Not providing state!");
         }

      } catch (StateTransferException ste) {
         throw ste;
      } catch (Exception e) {
         throw new StateTransferException(e);
      } finally {
         Util.flushAndCloseStream(oos);
         if (txLogActivated) transactionLog.deactivate();
      }
   }

   private void generateTransactionLog(ObjectOutputStream oos) throws Exception {
      // todo this should be configurable
      int maxNonProgressingLogWrites = 100;
      int flushTimeout = 60000;

      DistributedSync distributedSync = rpcManager.getTransport().getDistributedSync();

      try {
         if (trace) log.trace("Transaction log size is {0}", transactionLog.size());
         for (int nonProgress = 0, size = transactionLog.size(); size > 0;) {
            if (trace) log.trace("Tx Log remaining entries = " + size);
            transactionLog.writeCommitLog(marshaller, oos);
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
         delimit(oos);
         oos.flush();
         if (trace) log.trace("Waiting for a distributed sync block");
         distributedSync.blockUntilAcquired(flushTimeout, MILLISECONDS);

         if (trace) log.trace("Distributed sync block received, proceeding with writing commit log");
         // Write remaining transactions
         transactionLog.writeCommitLog(marshaller, oos);
         delimit(oos);

         // Write all non-completed prepares
         transactionLog.writePendingPrepares(marshaller, oos);
         delimit(oos);
         oos.flush();
      }
      finally {
         distributedSync.releaseProcessingLock();
      }
   }

   private void processCommitLog(ObjectInputStream ois) throws Exception {
      if (trace) log.trace("Applying commit log");
      Object object = marshaller.objectFromObjectStream(ois);
      while (object instanceof TransactionLog.LogEntry) {
         WriteCommand[] mods = ((TransactionLog.LogEntry) object).getModifications();
         if (trace) log.trace("Mods = {0}", Arrays.toString(mods));
         for (WriteCommand mod : mods) {
            commandsFactory.initializeReplicableCommand(mod);
            InvocationContext ctx = invocationContextContainer.get();
            ctx.setOriginLocal(false);
            ctx.setOptions(Options.CACHE_MODE_LOCAL, Options.SKIP_CACHE_STATUS_CHECK);
            interceptorChain.invoke(ctx, mod);
         }

         object = marshaller.objectFromObjectStream(ois);
      }

      assertDelimited(object);
      if (trace) log.trace("Finished applying commit log");
   }

   private void applyTransactionLog(ObjectInputStream ois) throws Exception {
      if (trace) log.trace("Integrating transaction log");

      processCommitLog(ois);
      stateSender = rpcManager.getCurrentStateTransferSource();
      mimicPartialFlushViaRPC(stateSender, true);
      needToUnblockRPC = true;

      try {
         if (trace)
            log.trace("Retrieving/Applying post-flush commits");
         processCommitLog(ois);

         if (trace)
            log.trace("Retrieving/Applying pending prepares");
         Object object = marshaller.objectFromObjectStream(ois);
         while (object instanceof PrepareCommand) {
            PrepareCommand command = (PrepareCommand) object;

            if (!transactionLog.hasPendingPrepare(command)) {
               if (trace) log.trace("Applying pending prepare {0}", command);
               commandsFactory.initializeReplicableCommand(command);
               InvocationContext ctx = invocationContextContainer.get();
               ctx.setOriginLocal(false);
               ctx.setOptions(Options.CACHE_MODE_LOCAL, Options.SKIP_CACHE_STATUS_CHECK);
               interceptorChain.invoke(ctx, command);
            } else {
               if (trace) log.trace("Prepare {0} not in tx log; not applying", command);
            }
            object = marshaller.objectFromObjectStream(ois);
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
      ObjectInputStream ois = null;
      try {
         ois = new ObjectInputStream(in);
         boolean canProvideState = (Boolean) marshaller.objectFromObjectStream(ois);
         if (canProvideState) {
            assertDelimited(ois);
            if (transientState) applyInMemoryState(ois);
            assertDelimited(ois);
            if (persistentState) applyPersistentState(ois);
            assertDelimited(ois);
            applyTransactionLog(ois);
            if (log.isDebugEnabled()) log.debug("State applied, closing object stream");
         } else {
            String msg = "Provider cannot provide state!";
            if (log.isDebugEnabled()) log.debug(msg);
            throw new StateTransferException(msg);
         }
      } catch (StateTransferException ste) {
         throw ste;
      } catch (Exception e) {
         throw new StateTransferException(e);
      } finally {
         // just close the object stream but do NOT close the underlying stream
         Util.closeStream(ois);
      }
   }

   private void applyInMemoryState(ObjectInputStream i) throws StateTransferException {
      dataContainer.clear();
      try {
         Set<InternalCacheEntry> set = (Set<InternalCacheEntry>) marshaller.objectFromObjectStream(i);
         for (InternalCacheEntry se : set)
            cache.put(se.getKey(), se.getValue(), se.getLifespan(), MILLISECONDS, Options.CACHE_MODE_LOCAL); // TODO store maxIdle as well
      } catch (Exception e) {
         dataContainer.clear();
         throw new StateTransferException(e);
      }
   }

   private void generateInMemoryState(ObjectOutputStream o) throws StateTransferException {
      // write all StoredEntries to the stream using the marshaller.
      // TODO is it safe enough to get these from the data container directly?
      try {
         Set<InternalCacheEntry> entries = new HashSet<InternalCacheEntry>();
         for (InternalCacheEntry e: dataContainer) {
            if (!e.isExpired()) entries.add(e);
         }
         if (log.isDebugEnabled()) log.debug("Writing {0} StoredEntries to stream", entries.size());
         marshaller.objectToObjectStream(entries, o);
      } catch (Exception e) {
         throw new StateTransferException(e);
      }
   }

   private void applyPersistentState(ObjectInputStream i) throws StateTransferException {
      try {
         // always use the unclosable stream delegate to ensure the impl doesn't close the stream
         cs.fromStream(new UnclosableObjectInputStream(i));
      } catch (CacheLoaderException cle) {
         throw new StateTransferException(cle);
      }
   }

   private void generatePersistentState(ObjectOutputStream o) throws StateTransferException {
      try {
         // always use the unclosable stream delegate to ensure the impl doesn't close the stream
         cs.toStream(new UnclosableObjectOutputStream(o));
      } catch (CacheLoaderException cle) {
         throw new StateTransferException(cle);
      }
   }

   private void delimit(ObjectOutputStream o) throws IOException {
      marshaller.objectToObjectStream(DELIMITER, o);
   }

   private void assertDelimited(ObjectInputStream i) throws StateTransferException {
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
