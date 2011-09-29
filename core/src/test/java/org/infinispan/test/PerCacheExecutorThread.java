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
package org.infinispan.test;

import org.infinispan.Cache;
import org.infinispan.distribution.rehash.XAResourceAdapter;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import javax.transaction.Transaction;
import javax.transaction.TransactionManager;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;

/**
 * Utility class that can be used for writing tests that need to access a cache instance from multiple threads.
 *
 * @author Mircea.Markus@jboss.com
 * @see Operations
 * @see OperationsResult
 */
public final class PerCacheExecutorThread extends Thread {

   private static final Log log = LogFactory.getLog(PerCacheExecutorThread.class);

   private Cache<Object, Object> cache;
   private BlockingQueue<Object> toExecute = new ArrayBlockingQueue<Object>(1);
   private volatile Object response;
   private CountDownLatch responseLatch = new CountDownLatch(1);
   private volatile Transaction ongoingTransaction;

   private volatile Object key, value;

   public void setKeyValue(Object key, Object value) {
      this.key = key;
      this.value = value;
   }

   public PerCacheExecutorThread(Cache<Object, Object> cache, int index) {
      super("PerCacheExecutorThread-" + index);
      this.cache = cache;
      start();
   }

   public Object execute(Operations op) {
      try {
         responseLatch = new CountDownLatch(1);
         toExecute.put(op);
         responseLatch.await();
         return response;
      } catch (InterruptedException e) {
         throw new RuntimeException("Unexpected", e);
      }
   }

   public void executeNoResponse(Operations op) {
      try {
         responseLatch = null;
         response = null;
         toExecute.put(op);
      } catch (InterruptedException e) {
         throw new RuntimeException("Unexpected", e);
      }
   }

   @Override
   public void run() {
      Operations operation;
      boolean run = true;
      while (run) {
         try {
            operation = (Operations) toExecute.take();
         } catch (InterruptedException e) {
            throw new RuntimeException(e);
         }
         log.tracef("about to process operation %s", operation);
         switch (operation) {
            case BEGGIN_TX: {
               TransactionManager txManager = TestingUtil.getTransactionManager(cache);
               try {
                  txManager.begin();
                  ongoingTransaction = txManager.getTransaction();
                  setResponse(OperationsResult.BEGGIN_TX_OK);
               } catch (Exception e) {
                  log.trace("Failure on beginning tx", e);
                  setResponse(e);
               }
               break;
            }
            case COMMIT_TX: {
               TransactionManager txManager = TestingUtil.getTransactionManager(cache);
               try {
                  txManager.commit();
                  ongoingTransaction = null;
                  setResponse(OperationsResult.COMMIT_TX_OK);
               } catch (Exception e) {
                  log.trace("Exception while committing tx", e);
                  setResponse(e);
               }
               break;
            }
            case PUT_KEY_VALUE: {
               try {
                  cache.put(key, value);
                  log.trace("Successfully executed putKeyValue(" + key + ", " + value + ")");
                  setResponse(OperationsResult.PUT_KEY_VALUE_OK);
               } catch (Exception e) {
                  log.trace("Exception while executing putKeyValue(" + key + ", " + value + ")", e);
                  setResponse(e);
               }
               break;
            }
            case REMOVE_KEY: {
               try {
                  cache.remove(key);
                  log.trace("Successfully executed remove(" + key + ")");
                  setResponse(OperationsResult.REMOVE_KEY_OK);
               } catch (Exception e) {
                  log.trace("Exception while executing remove(" + key + ")", e);
                  setResponse(e);
               }
               break;
            }
            case REPLACE_KEY_VALUE: {
               try {
                  cache.replace(key, value);
                  log.trace("Successfully executed replace(" + key + "," + value + ")");
                  setResponse(OperationsResult.REPLACE_KEY_VALUE_OK);
               } catch (Exception e) {
                  log.trace("Exception while executing replace(" + key + "," + value + ")", e);
                  setResponse(e);
               }
               break;
            }
            case FORCE2PC: {
               try {
                  TransactionManager txManager = TestingUtil.getTransactionManager(cache);
                  txManager.getTransaction().enlistResource(new XAResourceAdapter());
                  setResponse(OperationsResult.FORCE2PC_OK);
               } catch (Exception e) {
                  log.trace("Exception while executing replace(" + key + "," + value + ")", e);
                  setResponse(e);
               }
               break;
            }
            case STOP_THREAD: {
               log.trace("Exiting...");
               toExecute = null;
               run = false;
               break;
            }
            default : {
               setResponse(new IllegalStateException("Unknown operation!" + operation));
            }
         }
         if (responseLatch != null) responseLatch.countDown();
      }
   }

   private void setResponse(Object e) {
      log.tracef("setResponse to %s", e);
      response = e;
   }

   public void stopThread() {
      execute(Operations.STOP_THREAD);
      while (!this.getState().equals(State.TERMINATED)) {
         try {
            Thread.sleep(50);
         } catch (InterruptedException e) {
            throw new IllegalStateException(e);
         }
      }
   }

   public Object lastResponse() {
      return response;
   }

   public void clearResponse() {
      response = null;
   }

   public Object waitForResponse() {
      while (response == null) {
         try {
            Thread.sleep(50);
         } catch (InterruptedException e) {
            throw new RuntimeException(e);
         }
      }
      return response;
   }

   /**
    * Defines allowed operations for {@link PerCacheExecutorThread}.
    *
    * @author Mircea.Markus@jboss.com
    */
   public static enum Operations {
      BEGGIN_TX, COMMIT_TX, PUT_KEY_VALUE, REMOVE_KEY, REPLACE_KEY_VALUE, STOP_THREAD, FORCE2PC;
      public OperationsResult getCorrespondingOkResult() {
         switch (this) {
            case BEGGIN_TX:
               return OperationsResult.BEGGIN_TX_OK;
            case COMMIT_TX:
               return OperationsResult.COMMIT_TX_OK;
            case PUT_KEY_VALUE:
               return OperationsResult.PUT_KEY_VALUE_OK;
            case REMOVE_KEY:
               return OperationsResult.REMOVE_KEY_OK;
            case REPLACE_KEY_VALUE:
               return OperationsResult.REPLACE_KEY_VALUE_OK;
            case STOP_THREAD:
               return OperationsResult.STOP_THREAD_OK;
            case FORCE2PC:
               return OperationsResult.FORCE2PC_OK;
            default:
               throw new IllegalStateException("Unrecognized operation: " + this);
         }
      }

   }

   /**
    * Defines operation results returned by {@link PerCacheExecutorThread}.
    *
    * @author Mircea.Markus@jboss.com
    */
   public static enum OperationsResult {
      BEGGIN_TX_OK, COMMIT_TX_OK, PUT_KEY_VALUE_OK, REMOVE_KEY_OK, REPLACE_KEY_VALUE_OK, STOP_THREAD_OK , FORCE2PC_OK
   }

   public Transaction getOngoingTransaction() {
      return ongoingTransaction;
   }
}
