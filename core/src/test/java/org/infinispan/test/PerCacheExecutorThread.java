package org.infinispan.test;

import org.infinispan.Cache;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import javax.transaction.TransactionManager;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;

/**
 * Utility class that can be used for writing tests that need to access a cache instance from multiple threads.
 *
 * @author Mircea.Markus@jboss.com
 * @see Operations
 * @see PerCacheExecutorThread.OperationsResult
 */
public final class PerCacheExecutorThread extends Thread {

   private static Log log = LogFactory.getLog(PerCacheExecutorThread.class);

   private Cache<Object, Object> cache;
   private BlockingQueue<Object> toExecute = new ArrayBlockingQueue<Object>(1);
   private volatile Object response;
   private CountDownLatch responseLatch = new CountDownLatch(1);

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
         System.out.println("about to process operation " + operation);
         switch (operation) {
            case BEGGIN_TX: {
               TransactionManager txManager = TestingUtil.getTransactionManager(cache);
               try {
                  txManager.begin();
                  setResponse(OperationsResult.BEGGIN_TX_OK);
               } catch (Exception e) {
                  log.trace("Failure on beggining tx", e);
                  setResponse(e);
               }
               break;
            }
            case COMMIT_TX: {
               TransactionManager txManager = TestingUtil.getTransactionManager(cache);
               try {
                  txManager.commit();
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
                  log.trace("Successfully exucuted putKeyValue(" + key + ", " + value + ")");
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
                  log.trace("Successfully exucuted remove(" + key + ")");
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
                  log.trace("Successfully exucuted replace(" + key + "," + value + ")");
                  setResponse(OperationsResult.REPLACE_KEY_VALUE_OK);
               } catch (Exception e) {
                  log.trace("Exception while executing replace(" + key + "," + value + ")", e);
                  setResponse(e);
               }
               break;
            }
            case STOP_THREAD: {
               System.out.println("Exiting...");
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
      setResponse("EXIT");
   }

   private void setResponse(Object e) {
      log.trace("setResponse to " + e);
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
      BEGGIN_TX, COMMIT_TX, PUT_KEY_VALUE, REMOVE_KEY, REPLACE_KEY_VALUE, STOP_THREAD;
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
      BEGGIN_TX_OK, COMMIT_TX_OK, PUT_KEY_VALUE_OK, REMOVE_KEY_OK, REPLACE_KEY_VALUE_OK, STOP_THREAD_OK

   }
}
