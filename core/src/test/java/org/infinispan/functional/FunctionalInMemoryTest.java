package org.infinispan.functional;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.io.Serializable;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import org.infinispan.commons.CacheException;
import org.infinispan.commons.api.functional.EntryView.ReadEntryView;
import org.infinispan.commons.api.functional.EntryView.WriteEntryView;
import org.infinispan.remoting.RemoteException;
import org.testng.annotations.Test;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt; && Krzysztof Sobolewski &lt;Krzysztof.Sobolewski@atende.pl&gt;
 */
@Test(groups = "functional", testName = "functional.FunctionalInMemoryTest")
public class FunctionalInMemoryTest extends AbstractFunctionalOpTest {
   // As the functional API should not have side effects, it's hard to verify its execution when it does not
   // have any return value.
   static AtomicInteger invocationCount = new AtomicInteger();

   public FunctionalInMemoryTest() {
      persistence = false;
   }

   @Test(dataProvider = "owningModeAndMethod")
   public void testLoad(boolean isOwner, Method method) {
      Object key = getKey(isOwner);

      method.action.eval(key, wo, rw,
            (Consumer<ReadEntryView<Object, String>> & Serializable) view -> assertFalse(view.find().isPresent()),
            (Consumer<WriteEntryView<String>> & Serializable) view -> view.set("value"), () -> invocationCount);

      assertInvocations(2);

      caches(DIST).forEach(cache -> assertEquals(cache.get(key), "value", getAddress(cache).toString()));
      caches(DIST).forEach(cache -> {
         if (cache.getAdvancedCache().getDistributionManager().getLocality(key).isLocal()) {
            assertTrue(cache.getAdvancedCache().getDataContainer().containsKey(key), getAddress(cache).toString());
         } else {
            assertFalse(cache.getAdvancedCache().getDataContainer().containsKey(key), getAddress(cache).toString());
         }
      });

      method.action.eval(key, wo, rw,
            (Consumer<ReadEntryView<Object, String>> & Serializable) view -> {
               assertTrue(view.find().isPresent());
               assertEquals(view.get(), "value");
            },
            (Consumer<WriteEntryView<String>> & Serializable) view -> {
            }, () -> invocationCount);

      assertInvocations(4);
   }

   @Test(dataProvider = "methods")
   public void testLoadLocal(Method method) {
      Integer key = 1;

      method.action.eval(key, lwo, lrw,
            (Consumer<ReadEntryView<Integer, String>> & Serializable) view -> assertFalse(view.find().isPresent()),
            (Consumer<WriteEntryView<String>> & Serializable) view -> view.set("value"), () -> invocationCount);

      assertInvocations(1);
      assertEquals(cacheManagers.get(0).getCache().get(key), "value");

      method.action.eval(key, lwo, lrw,
            (Consumer<ReadEntryView<Integer, String>> & Serializable) view -> {
               assertTrue(view.find().isPresent());
               assertEquals(view.get(), "value");
            },
            (Consumer<WriteEntryView<String>> & Serializable) view -> {
            }, () -> invocationCount);

      assertInvocations(2);
   }

   @Test(dataProvider = "owningModeAndMethod")
   public void testExceptionPropagation(boolean isOwner, Method method) {
      Object key = getKey(isOwner);
      try {
         method.action.eval(key, wo, rw,
               (Consumer<ReadEntryView<Object, String>> & Serializable) view -> {
               },
               (Consumer<WriteEntryView<String>> & Serializable) view -> {
                  throw new TestException();
               }, () -> invocationCount);
         fail("Should throw CacheException:TestException");
      } catch (CacheException e) { // catches RemoteExceptions, too
         Throwable t = e;
         while (t.getCause() instanceof RemoteException) {
            t = t.getCause();
         }
         assertEquals(t.getCause().getClass(), TestException.class);
      }
   }

   @Test(dataProvider = "owningModeAndReadWrites")
   public void testMissingValue(boolean isOwner, Method method) {
      Object key = getKey(isOwner);
      try {
         method.action.eval(key, null, rw,
               (Consumer<ReadEntryView<Object, String>> & Serializable) view -> view.get(),
               (Consumer<WriteEntryView<String>> & Serializable) view -> {
               }, () -> invocationCount);
         fail("Should throw CacheException:NoSuchElem entException");
      } catch (CacheException e) { // catches RemoteExceptions, too
         Throwable t = e;
         while (t.getCause() instanceof RemoteException) {
            t = t.getCause();
         }
         assertEquals(t.getCause().getClass(), NoSuchElementException.class);
      }
   }

   @Override
   protected AtomicInteger invocationCount() {
      return invocationCount;
   }

   private static class TestException extends RuntimeException {
   }
}
