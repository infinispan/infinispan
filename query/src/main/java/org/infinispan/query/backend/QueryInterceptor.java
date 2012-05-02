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

import org.hibernate.search.backend.TransactionContext;
import org.hibernate.search.backend.spi.Work;
import org.hibernate.search.backend.spi.WorkType;
import org.hibernate.search.engine.spi.EntityIndexBinder;
import org.hibernate.search.spi.SearchFactoryIntegrator;
import org.infinispan.commands.write.ClearCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.marshall.MarshalledValue;
import org.infinispan.query.Transformer;
import org.infinispan.query.logging.Log;
import org.infinispan.util.concurrent.ConcurrentMapFactory;
import org.infinispan.util.logging.LogFactory;

import javax.transaction.TransactionManager;
import javax.transaction.TransactionSynchronizationRegistry;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

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
   public void injectDependencies(
         @ComponentName(KnownComponentNames.ASYNC_TRANSPORT_EXECUTOR) ExecutorService e) {
      this.asyncExecutor = e;
   }

   protected boolean shouldModifyIndexes(InvocationContext ctx) {
      return ! ctx.hasFlag(Flag.SKIP_INDEXING);
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

      // This method will get the put() calls on the cache and then send them into Lucene once it's successful.
      // do the actual put first.
      Object toReturn = invokeNextInterceptor(ctx, command);

      if (shouldModifyIndexes(ctx)) {
         // First making a check to see if the key is already in the cache or not. If it isn't we can add the key no problem,
         // otherwise we need to be updating the indexes as opposed to simply adding to the indexes.
         getLog().debug("Infinispan Query indexing is triggered");
         Object key = command.getKey();
         Object value = extractValue(command.getValue());

         if (updateKnownTypesIfNeeded(value)) {
            // This means that the entry is just modified so we need to update the indexes and not add to them.
            updateIndexes(value, extractValue(key));
         }
         else {
            if (updateKnownTypesIfNeeded(toReturn)) {
               removeFromIndexes(toReturn, extractValue(command.getKey()));
            }
         }
      }
      return toReturn;
   }

   @Override
   public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
      // remove the object out of the cache first.
      Object valueRemoved = invokeNextInterceptor(ctx, command);

      if (command.isSuccessful() && !command.isNonExistent() && shouldModifyIndexes(ctx)) {
         Object value = extractValue(valueRemoved);
         if (updateKnownTypesIfNeeded( value )) {
            removeFromIndexes(value, extractValue(command.getKey()));
         }
      }
      return valueRemoved;
   }

   @Override
   public Object visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) throws Throwable {
      Object valueReplaced = invokeNextInterceptor(ctx, command);
      if (valueReplaced != null && command.isSuccessful() && shouldModifyIndexes(ctx)) {

         Object[] parameters = command.getParameters();
         Object p1 = extractValue(parameters[1]);
         Object p2 = extractValue(parameters[2]);
         boolean originalIsIndexed = updateKnownTypesIfNeeded( p1 );
         boolean newValueIsIndexed = updateKnownTypesIfNeeded( p2 );
         Object key = extractValue(command.getKey());
         
         if (p1 != null && originalIsIndexed) {
            removeFromIndexes(p1, key);
         }
         if (newValueIsIndexed) {
            updateIndexes(p2, key);
         }
      }

      return valueReplaced;
   }

   @Override
   public Object visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
      Object mapPut = invokeNextInterceptor(ctx, command);

      if (shouldModifyIndexes(ctx)) {
         Map<Object, Object> dataMap = command.getMap();

         // Loop through all the keys and put those key, value pairings into lucene.

         for (Map.Entry<Object, Object> entry : dataMap.entrySet()) {
            Object value = extractValue(entry.getValue());
            if (updateKnownTypesIfNeeded(value)) {
               updateIndexes(value, extractValue(entry.getKey()));
            }
         }
      }
      return mapPut;
   }

   @Override
   public Object visitClearCommand(InvocationContext ctx, ClearCommand command) throws Throwable {

      // This method is called when somebody calls a cache.clear() and we will need to wipe everything in the indexes.
      Object returnValue = invokeNextInterceptor(ctx, command);

      if (shouldModifyIndexes(ctx)) {
         if (getLog().isTraceEnabled()) getLog().trace("shouldModifyIndexes() is true and we can clear the indexes");

         for (Class c : this.knownClasses.keySet()) {
            EntityIndexBinder binder = this.searchFactory.getIndexBindingForEntity(c);
            if ( binder != null ) { //check as not all known classes are indexed
               searchFactory.getWorker().performWork(new Work<Object>(c, (Serializable)null,
                     WorkType.PURGE_ALL), new TransactionalEventTransactionContext(transactionManager, transactionSynchronizationRegistry));
            }
         }
      }
      return returnValue;
   }

   // Method that will be called when data needs to be removed from Lucene.
   protected void removeFromIndexes(Object value, Object key) {

      // The key here is the String representation of the key that is stored in the cache.
      // The key is going to be the documentID for Lucene.
      // The object parameter is the actual value that needs to be removed from lucene.
      if (value == null) throw new NullPointerException("Cannot handle a null value!");
      TransactionContext transactionContext = new TransactionalEventTransactionContext(transactionManager, transactionSynchronizationRegistry);
      searchFactory.getWorker().performWork(new Work<Object>(value, keyToString(key), WorkType.DELETE), transactionContext);
   }

   protected void updateIndexes(Object value, Object key){
      // The key here is the String representation of the key that is stored in the cache.
      // The key is going to be the documentID for Lucene.
      // The object parameter is the actual value that needs to be removed from lucene.
      if (value == null) throw new NullPointerException("Cannot handle a null value!");
      TransactionContext transactionContext = new TransactionalEventTransactionContext(transactionManager, transactionSynchronizationRegistry);
      searchFactory.getWorker().performWork(new Work<Object>(value, keyToString(key), WorkType.UPDATE), transactionContext);
   }

   private Object extractValue(Object wrappedValue) {
      if (wrappedValue instanceof MarshalledValue)
         return ((MarshalledValue) wrappedValue).get();
      else
         return wrappedValue;
   }

   public void enableClasses(Class<?>[] classes) {
      if ( classes == null || classes.length == 0 ) {
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
         Set<Class<?>> existingClasses = knownClasses.keySet();
         int index = existingClasses.size();
         Class<?>[] all = existingClasses.toArray(new Class[existingClasses.size()+toAdd.size()]);
         for (Class<?> toAddClass : toAdd) {
            all[index++] = toAddClass;
         }
         searchFactory.addClasses(all);
         for (Class<?> type : toAdd) {
            if (searchFactory.getIndexBindingForEntity(type) != null) {
               knownClasses.put(type, Boolean.TRUE);
            }
            else {
               knownClasses.put(type, Boolean.FALSE);
            }
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

   private boolean updateKnownTypesIfNeeded(Object value) {
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
}
