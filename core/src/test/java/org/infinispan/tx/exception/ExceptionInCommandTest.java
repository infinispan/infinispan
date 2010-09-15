package org.infinispan.tx.exception;

import org.infinispan.atomic.AtomicHashMap;
import org.infinispan.atomic.Delta;
import org.infinispan.atomic.DeltaAware;
import org.infinispan.config.Configuration;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.Test;

import javax.transaction.RollbackException;
import javax.transaction.Status;
import java.io.Serializable;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.2
 */
@Test(groups = "functional", testName = "tx.exception.ExceptionInCommandTest")
public class ExceptionInCommandTest extends MultipleCacheManagersTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      addClusterEnabledCacheManagers(Configuration.CacheMode.REPL_SYNC, 2);
      waitForClusterToForm();
   }                                                                        

   public void testPutThrowsLocalException() throws Exception {
      tm(0).begin();

      Delta d = new Delta() {
         public DeltaAware merge(DeltaAware d) {
            throw new RuntimeException("Induced!");
         }
      };

      try {
         cache(0).put("k", d);
         assert false;
      } catch (RuntimeException e) {
         assert tx(0).getStatus() == Status.STATUS_MARKED_ROLLBACK;
      }
   }

   @Test (expectedExceptions = RollbackException.class)
   public void testPutThrowsRemoteException() throws Exception {
      tm(0).begin();

      MyDelta d = new MyDelta();
      d.setCreator();

      cache(0).put("k", d);

      tm(0).commit();

   }

   private static Log log = LogFactory.getLog(ExceptionInCommandTest.class);

   private static class MyDelta implements Delta , Serializable {
      transient Thread creator;

      public void setCreator() {creator = Thread.currentThread();}

      public DeltaAware merge(DeltaAware d) {
         log.trace("Creator == " );

         if (creator != Thread.currentThread())
            throw new RuntimeException("Induced!");
         return new AtomicHashMap();
      }
   }
}
