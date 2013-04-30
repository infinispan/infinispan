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
package org.infinispan.query.backend;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import javax.transaction.TransactionManager;
import javax.transaction.TransactionSynchronizationRegistry;

import org.hibernate.search.backend.TransactionContext;
import org.hibernate.search.backend.spi.Work;
import org.hibernate.search.backend.spi.WorkType;
import org.hibernate.search.backend.spi.Worker;
import org.hibernate.search.engine.spi.EntityIndexBinder;
import org.hibernate.search.spi.SearchFactoryIntegrator;
import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.write.ClearCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.marshall.MarshalledValue;
import org.infinispan.query.Transformer;
import org.infinispan.query.impl.DefaultSearchWorkCreator;
import org.infinispan.query.logging.Log;
import org.infinispan.util.concurrent.ConcurrentMapFactory;
import org.infinispan.util.logging.LogFactory;

/**
 * This interceptor will be created when the System Property "infinispan.query.indexLocalOnly" is "false"
 * <p/>
 * This type of interceptor will allow the indexing of data even when it comes from other caches within a cluster.
 * <p/>
 * However, if the a cache would not be putting the data locally, the interceptor will not index it.
 *
 * @author Navin Surtani
 * @author Sanne Grinovero <sanne@hibernate.org> (C) 2011 Red Hat Inc.
 * @author Marko Luksa
 * @since 4.0
 */
public class QueryInterceptor extends CommandInterceptor {

   private final SearchFactoryIntegrator searchFactory;
   private final ConcurrentMap<Class<?>,Boolean> knownClasses = ConcurrentMapFactory.makeConcurrentMap();
   private final Lock mutating = new ReentrantLock();
   private final KeyTransformationHandler keyTransformationHandler = new KeyTransformationHandler();
   private SearchWorkCreator<Object> searchWorkCreator = new DefaultSearchWorkCreator<Object>();

   private DataContainer dataContainer;
   protected TransactionManager transactionManager;
   protected TransactionSynchronizationRegistry transactionSynchronizationRegistry;
   protected ExecutorService asyncExecutor;

   private static final Log log = LogFactory.getLog(QueryInterceptor.class, Log.class);

   @Override
   protected Log getLog() {
      return log;
   }

   public QueryInterceptor(SearchFactoryIntegrator searchFactory) {
      this.searchFactory = searchFactory;
   }

   @Inject
   @SuppressWarnings("unused")
   public void injectDependencies(TransactionManager transactionManager,
                                  TransactionSynchronizationRegistry transactionSynchronizationRegistry,
                                  DataContainer dataContainer,
                                  @ComponentName(KnownComponentNames.ASYNC_TRANSPORT_EXECUTOR) ExecutorService e) {
      // Fields on superclass.
      this.transactionManager = transactionManager;
      this.transactionSynchronizationRegistry = transactionSynchronizationRegistry;
      this.asyncExecutor = e;
      this.dataContainer = dataContainer;
   }

   protected boolean shouldModifyIndexes(FlagAffectedCommand command, InvocationContext ctx) {
      return !command.hasFlag(Flag.SKIP_INDEXING);
   }

   /**
    * Use this executor for Async operations
    * @return
    */
   public ExecutorService getAsyncExecutor() {
      return asyncExecutor;
   }

   @Override
   public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
      Object toReturn = invokeNextInterceptor(ctx, command);
      processPutKeyValueCommand(command, ctx, toReturn, null);
      return toReturn;
   }

   @Override
   public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
      // remove the object out of the cache first.
      Object valueRemoved = invokeNextInterceptor(ctx, command);
      processRemoveCommand(command, ctx, valueRemoved, null);
      return valueRemoved;
   }

   @Override
   public Object visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) throws Throwable {
      Object valueReplaced = invokeNextInterceptor(ctx, command);
      processReplaceCommand(command, ctx, valueReplaced, null);
      return valueReplaced;
   }

   @Override
   public Object visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
      Object mapPut = invokeNextInterceptor(ctx, command);
      processPutMapCommand(command, ctx, null);
      return mapPut;
   }

   @Override
   public Object visitClearCommand(final InvocationContext ctx, final ClearCommand command) throws Throwable {
      // This method is called when somebody calls a cache.clear() and we will need to wipe everything in the indexes.
      Object returnValue = invokeNextInterceptor(ctx, command);
      processClearCommand(command, ctx, null);
      return returnValue;
   }

   /**
    * Remove all entries from all known indexes
    */
   public void purgeAllIndexes() {
      purgeAllIndexes(null);
   }

   private void purgeAllIndexes(TransactionContext transactionContext) {
      transactionContext = transactionContext == null ? makeTransactionalEventContext() : transactionContext;
      for (Class c : this.knownClasses.keySet()) {
         if (isIndexed(c)) {
            //noinspection unchecked
            performSearchWorks(searchWorkCreator.createPerEntityTypeWorks(c, WorkType.PURGE_ALL), transactionContext);
         }
      }
   }

   // Method that will be called when data needs to be removed from Lucene.
   protected void removeFromIndexes(final Object value, final Object key, final TransactionContext transactionContext) {
      performSearchWork(value, keyToString(key), WorkType.DELETE, transactionContext);
   }

   protected void updateIndexes(Object value, Object key, TransactionContext transactionContext) {
      performSearchWork(value, keyToString(key), WorkType.UPDATE, transactionContext);
   }

   private void performSearchWork(Object value, Serializable id, WorkType workType, TransactionContext transactionContext) {
      if (value == null) throw new NullPointerException("Cannot handle a null value!");
      Collection<Work<Object>> works = searchWorkCreator.createPerEntityWorks(value, id, workType);
      performSearchWorks(works, transactionContext);
   }

   private <T> void performSearchWorks(Collection<Work<T>> works, TransactionContext transactionContext) {
      Worker worker = searchFactory.getWorker();
      for (Work<T> work : works) {
         worker.performWork(work, transactionContext);
      }
   }

   private boolean isIndexed(Class<?> c) {
      EntityIndexBinder binder = this.searchFactory.getIndexBindingForEntity(c);
      return binder != null;
   }

   private Object extractValue(Object wrappedValue) {
      if (wrappedValue instanceof MarshalledValue)
         return ((MarshalledValue) wrappedValue).get();
      else
         return wrappedValue;
   }

   public void enableClasses(Class<?>[] classes) {
      if (classes == null || classes.length == 0) {
         return;
      }
      enableClassesIncrementally(classes, false);
   }

   private void enableClassesIncrementally(Class<?>[] classes, boolean locked) {
      ArrayList<Class<?>> toAdd = null;
      for (Class<?> type : classes) {
         if (!knownClasses.containsKey(type)) {
            if (toAdd==null)
               toAdd = new ArrayList<Class<?>>(classes.length);
            toAdd.add(type);
         }
      }
      if (toAdd == null) {
         return;
      }
      if (locked) {
         Class[] array = toAdd.toArray(new Class[toAdd.size()]);
         searchFactory.addClasses(array);
         for (Class<?> type : toAdd) {
            knownClasses.put(type, isIndexed(type));
         }
      } else {
         mutating.lock();
         try {
            enableClassesIncrementally(classes, true);
         } finally {
            mutating.unlock();
         }
      }
   }

   public boolean updateKnownTypesIfNeeded(Object value) {
      if ( value != null ) {
         Class<?> potentialNewType = value.getClass();
         if ( ! this.knownClasses.containsKey(potentialNewType) ) {
            mutating.lock();
            try {
               enableClassesIncrementally( new Class[]{potentialNewType}, true);
            }
            finally {
               mutating.unlock();
            }
         }
         return this.knownClasses.get(potentialNewType);
      }
      else {
         return false;
      }
   }

   public void registerKeyTransformer(Class<?> keyClass, Class<? extends Transformer> transformerClass) {
      keyTransformationHandler.registerTransformer(keyClass, transformerClass);
   }

   private String keyToString(Object key) {
      return keyTransformationHandler.keyToString(key);
   }

   public KeyTransformationHandler getKeyTransformationHandler() {
      return keyTransformationHandler;
   }

   public void enableClasses(Set<Class> knownIndexedTypes) {
      Class[] classes = knownIndexedTypes.toArray(new Class[knownIndexedTypes.size()]);
      enableClasses(classes);
   }

   public SearchFactoryIntegrator getSearchFactory() {
      return searchFactory;
   }

   public void setSearchWorkCreator(SearchWorkCreator<Object> searchWorkCreator) {
      this.searchWorkCreator = searchWorkCreator;
   }

   public SearchWorkCreator<Object> getSearchWorkCreator() {
      return searchWorkCreator;
   }

   /**
    * In case of a remotely originating transactions we don't have a chance to visit the single
    * commands but receive this "batch". We then need the before-apply snapshot of some types
    * to route the cleanup commands to the correct indexes.
    * Note we don't need to visit the CommitCommand as the indexing context is registered
    * as a transaction sync.
    */
   @Override
   public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
      final WriteCommand[] writeCommands = command.getModifications();
      final Object[] stateBeforePrepare = new Object[writeCommands.length];

      for (int i=0; i<writeCommands.length; i++) {
         final WriteCommand writeCommand = writeCommands[i];
         if (writeCommand instanceof PutKeyValueCommand) {
            InternalCacheEntry internalCacheEntry = dataContainer.get(((PutKeyValueCommand) writeCommand).getKey());
            stateBeforePrepare[i] = internalCacheEntry != null ? internalCacheEntry.getValue() : null;
         }
         else if (writeCommand instanceof PutMapCommand) {
            //think about this: ISPN-2478
         }
         else if (writeCommand instanceof RemoveCommand) {
            InternalCacheEntry internalCacheEntry = dataContainer.get(((RemoveCommand) writeCommand).getKey());
            stateBeforePrepare[i] = internalCacheEntry != null ? internalCacheEntry.getValue() : null;
         }
         else if (writeCommand instanceof ReplaceCommand) {
            InternalCacheEntry internalCacheEntry = dataContainer.get(((ReplaceCommand) writeCommand).getKey());
            stateBeforePrepare[i] = internalCacheEntry != null ? internalCacheEntry.getValue() : null;
         }
      }

      final Object toReturn = super.visitPrepareCommand(ctx, command);

      if (ctx.isTransactionValid()) {
         final TransactionContext transactionContext = makeTransactionalEventContext();
         for (int i=0; i<writeCommands.length; i++) {
            final WriteCommand writeCommand = writeCommands[i];
            if (writeCommand instanceof PutKeyValueCommand) {
               processPutKeyValueCommand((PutKeyValueCommand) writeCommand, ctx, stateBeforePrepare[i], transactionContext);
            }
            else if (writeCommand instanceof PutMapCommand) {
               //FIXME ISPN-2478
               processPutMapCommand((PutMapCommand) writeCommand, ctx, transactionContext);
            }
            else if (writeCommand instanceof RemoveCommand) {
               processRemoveCommand((RemoveCommand) writeCommand, ctx, stateBeforePrepare[i], transactionContext);
            }
            else if (writeCommand instanceof ReplaceCommand) {
               processReplaceCommand((ReplaceCommand) writeCommand, ctx, stateBeforePrepare[i], transactionContext);
            }
            else if (writeCommand instanceof ClearCommand) {
               processClearCommand((ClearCommand)writeCommand, ctx, transactionContext);
            }
         }
      }
      return toReturn;
   }

   /**
    * Indexing management of a ReplaceCommand
    *
    * @param command the ReplaceCommand
    * @param ctx the InvocationContext
    * @param valueReplaced the previous value on this key
    * @param transactionContext Optional for lazy initialization, or reuse an existing context.
    */
   private void processReplaceCommand(final ReplaceCommand command, final InvocationContext ctx, final Object valueReplaced, TransactionContext transactionContext) {
      if (valueReplaced != null && command.isSuccessful() && shouldModifyIndexes(command, ctx)) {
         Object[] parameters = command.getParameters();
         Object p1 = extractValue(parameters[1]);
         Object p2 = extractValue(parameters[2]);
         boolean originalIsIndexed = updateKnownTypesIfNeeded( p1 );
         boolean newValueIsIndexed = updateKnownTypesIfNeeded( p2 );
         Object key = extractValue(command.getKey());

         if (p1 != null && originalIsIndexed) {
            transactionContext = transactionContext == null ? makeTransactionalEventContext() : transactionContext;
            removeFromIndexes(p1, key, transactionContext);
         }
         if (newValueIsIndexed) {
            transactionContext = transactionContext == null ? makeTransactionalEventContext() : transactionContext;
            updateIndexes(p2, key, transactionContext);
         }
      }
   }

   /**
    * Indexing management of a RemoveCommand
    *
    * @param command the visited RemoveCommand
    * @param ctx the InvocationContext of the RemoveCommand
    * @param valueRemoved the value before the removal
    * @param transactionContext Optional for lazy initialization, or reuse an existing context.
    */
   private void processRemoveCommand(final RemoveCommand command, final InvocationContext ctx, final Object valueRemoved, TransactionContext transactionContext) {
      if (command.isSuccessful() && !command.isNonExistent() && shouldModifyIndexes(command, ctx)) {
         final Object value = extractValue(valueRemoved);
         if (updateKnownTypesIfNeeded(value)) {
            transactionContext = transactionContext == null ? makeTransactionalEventContext() : transactionContext;
            removeFromIndexes(value, extractValue(command.getKey()), transactionContext);
         }
      }
   }

   /**
    * Indexing management of a PutMapCommand
    *
    * @param command the visited PutMapCommand
    * @param ctx the InvocationContext of the PutMapCommand
    * @param transactionContext
    */
   private void processPutMapCommand(final PutMapCommand command, final InvocationContext ctx, TransactionContext transactionContext) {
      if (shouldModifyIndexes(command, ctx)) {
         Map<Object, Object> dataMap = command.getMap();
         // Loop through all the keys and put those key-value pairings into lucene.
         for (Map.Entry<Object, Object> entry : dataMap.entrySet()) {
            final Object value = extractValue(entry.getValue());
            if (updateKnownTypesIfNeeded(value)) {
               transactionContext = transactionContext == null ? makeTransactionalEventContext() : transactionContext;
               updateIndexes(value, extractValue(entry.getKey()), transactionContext);
            }
         }
      }
   }

   /**
    * Indexing management of a PutKeyValueCommand
    *
    * @param command the visited PutKeyValueCommand
    * @param ctx the InvocationContext of the PutKeyValueCommand
    * @param previousValue the value being replaced by the put operation
    * @param transactionContext Optional for lazy initialization, or reuse an existing context.
    */
   private void processPutKeyValueCommand(final PutKeyValueCommand command, final InvocationContext ctx, final Object previousValue, TransactionContext transactionContext) {
      //whatever the new type, we might still need to cleanup for the previous value (and schedule removal first!)
      if (updateKnownTypesIfNeeded(previousValue)) {
         if (shouldModifyIndexes(command, ctx)) {
            transactionContext = transactionContext == null ? makeTransactionalEventContext() : transactionContext;
            removeFromIndexes(previousValue, extractValue(command.getKey()), transactionContext);
         }
      }
      Object value = extractValue(command.getValue());
      if (updateKnownTypesIfNeeded(value)) {
         if (shouldModifyIndexes(command, ctx)) {
            // This means that the entry is just modified so we need to update the indexes and not add to them.
            transactionContext = transactionContext == null ? makeTransactionalEventContext() : transactionContext;
            updateIndexes(value, extractValue(command.getKey()), transactionContext);
         }
      }
   }

   /**
    * Indexing management of the Clear command
    *
    * @param command the ClearCommand
    * @param ctx the InvocationContext of the PutKeyValueCommand
    * @param transactionContext Optional for lazy initialization, or to reuse an existing transactional context.
    */
   private void processClearCommand(final ClearCommand command, final InvocationContext ctx, TransactionContext transactionContext) {
      if (shouldModifyIndexes(command, ctx)) {
         purgeAllIndexes(transactionContext);
      }
   }

   private final TransactionContext makeTransactionalEventContext() {
      return new TransactionalEventTransactionContext(transactionManager, transactionSynchronizationRegistry);
   }

}
