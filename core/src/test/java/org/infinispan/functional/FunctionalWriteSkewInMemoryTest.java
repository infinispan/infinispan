package org.infinispan.functional;

import static org.infinispan.test.Exceptions.assertException;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertTrue;

import java.util.Collections;
import java.util.concurrent.CompletionException;
import java.util.stream.Stream;

import javax.transaction.RollbackException;
import javax.transaction.Status;
import javax.transaction.Transaction;
import javax.transaction.xa.XAException;

import org.infinispan.Cache;
import org.infinispan.remoting.RemoteException;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.WriteSkewException;
import org.infinispan.util.concurrent.IsolationLevel;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "functional")
public class FunctionalWriteSkewInMemoryTest extends FunctionalTxInMemoryTest {

   public FunctionalWriteSkewInMemoryTest() {
      transactional(true).lockingMode(LockingMode.OPTIMISTIC).isolationLevel(IsolationLevel.REPEATABLE_READ);
   }

   @Override
   protected String parameters() {
      return null;
   }

   @DataProvider(name = "readCombos")
   public static Object[][] readCombos() {
      return Stream.of(Boolean.TRUE, Boolean.FALSE).flatMap(
            isOwner -> Stream.of(ReadOp.values()).flatMap(
                  op1 -> Stream.of(ReadOp.values()).map(
                        op2 -> new Object[]{isOwner, op1, op2})))
            .filter(args -> ((ReadOp) args[1]).isFunctional() || ((ReadOp) args[2]).isFunctional())
            .toArray(Object[][]::new);
   }

   @Test(dataProvider = "readCombos")
   public void testWriteSkew(boolean isOwner, ReadOp op1, ReadOp op2) throws Throwable {
      Object key = getKey(isOwner, DIST);
      cache(0, DIST).put(key, "value0");

      tm.begin();
      assertEquals("value0", op1.action.eval(cache(0, DIST), key, ro, rw));
      Transaction transaction = tm.suspend();

      cache(0, DIST).put(key, "value1");

      tm.resume(transaction);
      try {
         assertEquals("value0", op2.action.eval(cache(0, DIST), key, ro, rw));
      } catch (CompletionException e) {
         assertException(WriteSkewException.class, e.getCause());
         // this is fine; either we read the old value, or we can't read it and we throw
         tm.rollback();
      } catch (WriteSkewException e) {
         // synchronous get is invoked using synchronous API, without wrapping into CompletionExceptions
         assert op2 == ReadOp.GET;
      }
      if (tm.getStatus() == Status.STATUS_ACTIVE) {
         try {
            tm.commit();
         } catch (RollbackException e) {
            Throwable[] suppressed = e.getSuppressed();
            assertTrue(suppressed != null && suppressed.length == 1);
            assertEquals(XAException.class, suppressed[0].getClass());
            Throwable cause = suppressed[0].getCause();
            while (cause instanceof RemoteException) {
               cause = cause.getCause();
            }
            assertNotNull(cause);
            assertEquals(WriteSkewException.class, cause.getClass());
         }
      }
   }

   enum ReadOp {
      READ(true, (cache, key, ro, rw) -> ro.eval(key, EntryView.ReadEntryView::get).join()),
      READ_MANY(true, (cache, key, ro, rw) -> ro.evalMany(Collections.singleton(key), EntryView.ReadEntryView::get).findAny().get()),
      READ_WRITE_KEY(true, (cache, key, ro, rw) -> rw.eval(key, EntryView.ReadEntryView::get).join()),
      READ_WRITE_KEY_VALUE(true, (cache, key, ro, rw) -> rw.eval(key, null, (value, v) -> v.get()).join()),
      READ_WRITE_MANY(true, (cache, key, ro, rw) -> rw.evalMany(Collections.singleton(key), EntryView.ReadEntryView::get).findAny().get()),
      READ_WRITE_MANY_ENTRIES(true, (cache, key, ro, rw) -> rw.evalMany(Collections.singletonMap(key, null), (value, v) -> v.get()).findAny().get()),
      GET(false, (cache, key, ro, rw) -> cache.get(key))
      ;

      final Performer action;
      final boolean functional;

      ReadOp(boolean functional, Performer action) {
         this.functional = functional;
         this.action = action;
      }

      public boolean isFunctional() {
         return functional;
      }

      @FunctionalInterface
      private interface Performer {
         Object eval(Cache cache, Object key, FunctionalMap.ReadOnlyMap<Object, String> ro, FunctionalMap.ReadWriteMap<Object, String> rw) throws Throwable;
      }
   }
}
