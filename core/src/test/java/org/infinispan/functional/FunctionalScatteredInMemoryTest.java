package org.infinispan.functional;

import static org.infinispan.test.Exceptions.assertException;
import static org.infinispan.test.Exceptions.assertExceptionNonStrict;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.util.NoSuchElementException;
import java.util.concurrent.CompletionException;

import org.infinispan.commons.CacheException;
import org.infinispan.configuration.cache.BiasAcquisition;
import org.infinispan.remoting.RemoteException;
import org.infinispan.scattered.Utils;
import org.infinispan.test.TestException;
import org.testng.annotations.Test;

public class FunctionalScatteredInMemoryTest extends AbstractFunctionalOpTest {
   @Override
   public Object[] factory() {
      return new Object[] {
            new FunctionalScatteredInMemoryTest().biasAcquisition(BiasAcquisition.NEVER),
            new FunctionalScatteredInMemoryTest().biasAcquisition(BiasAcquisition.ON_WRITE)
      };
   }

   @Test(dataProvider = "owningModeAndWriteMethod")
   public void testWrite(boolean isOwner, WriteMethod method) {
      Object key = getKey(isOwner, SCATTERED);

      method.eval(key, swo, srw,
            view -> { assertFalse(view.find().isPresent()); return null; },
            (view, nil) -> view.set("value"), getClass());

      assertInvocations(1);

      caches(SCATTERED).forEach(cache -> assertEquals(cache.get(key), "value", getAddress(cache).toString()));
      Utils.assertOwnershipAndNonOwnership(caches(SCATTERED), key);

      method.eval(key, swo, srw,
            view -> {
               assertTrue(view.find().isPresent());
               assertEquals(view.get(), "value");
               return null;
            },
            (view, nil) -> {}, getClass());

      assertInvocations(2);
   }

   @Test(dataProvider = "owningModeAndWriteMethod")
   public void testExceptionPropagation(boolean isOwner, WriteMethod method) {
      Object key = getKey(isOwner, SCATTERED);
      try {
         method.eval(key, swo, srw,
               view -> null,
               (view, nil) -> {
                  throw new TestException();
               }, getClass());
         fail("Should throw CompletionException:CacheException:[RemoteException:]*TestException");
      } catch (CacheException | CompletionException e) { // catches RemoteExceptions, too
         Throwable t = e;
         assertException(CompletionException.class, t);
         t = t.getCause();
         assertExceptionNonStrict(CacheException.class, t);
         while (t.getCause() instanceof RemoteException && t != t.getCause()) {
            t = t.getCause();
         }
         assertException(TestException.class, t.getCause());
      }
   }

   @Test(dataProvider = "owningModeAndReadWrites")
   public void testWriteOnMissingValue(boolean isOwner, WriteMethod method) {
      Object key = getKey(isOwner, SCATTERED);
      try {
         method.eval(key, null, srw,
               view -> view.get(),
               (view, nil) -> {}, getClass());
         fail("Should throw CompletionException:CacheException:[RemoteException:]*NoSuchElementException");
      } catch (CompletionException e) { // catches RemoteExceptions, too
         Throwable t = e;
         assertException(CompletionException.class, t);
         t = t.getCause();
         assertExceptionNonStrict(CacheException.class, t);
         while (t.getCause() instanceof RemoteException && t != t.getCause()) {
            t = t.getCause();
         }
         assertException(NoSuchElementException.class, t.getCause());
      }
   }

   @Test(dataProvider = "owningModeAndReadMethod")
   public void testReadLoad(boolean isOwner, ReadMethod method) {
      Object key = getKey(isOwner, SCATTERED);

      assertTrue(method.eval(key, sro, view -> { assertFalse(view.find().isPresent()); return true; }));

      // we can't add from read-only cache, so we put manually:
      cache(0, SCATTERED).put(key, "value");

      caches(SCATTERED).forEach(cache -> assertEquals(cache.get(key), "value", getAddress(cache).toString()));
      Utils.assertOwnershipAndNonOwnership(caches(SCATTERED), key);

      assertEquals(method.eval(key, sro,
            view -> {
               assertTrue(view.find().isPresent());
               assertEquals(view.get(), "value");
               return "OK";
            }), "OK");
   }

   @Test(dataProvider = "owningModeAndReadMethod")
   public void testReadOnMissingValue(boolean isOwner, ReadMethod method) {
      testReadOnMissingValue(getKey(isOwner, SCATTERED), sro, method);
   }

}
