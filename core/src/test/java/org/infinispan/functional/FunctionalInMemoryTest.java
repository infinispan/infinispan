package org.infinispan.functional;

import static org.infinispan.commons.test.Exceptions.assertException;
import static org.infinispan.commons.test.Exceptions.assertExceptionNonStrict;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.util.NoSuchElementException;
import java.util.concurrent.CompletionException;

import javax.transaction.xa.XAException;

import org.infinispan.Cache;
import org.infinispan.commons.CacheException;
import org.infinispan.functional.impl.ReadOnlyMapImpl;
import org.infinispan.remoting.RemoteException;
import org.infinispan.test.TestException;
import org.testng.annotations.Test;

import jakarta.transaction.RollbackException;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt; and Krzysztof Sobolewski &lt;Krzysztof.Sobolewski@atende.pl&gt;
 */
@Test(groups = "functional", testName = "functional.FunctionalInMemoryTest")
public class FunctionalInMemoryTest extends AbstractFunctionalOpTest {

   public FunctionalInMemoryTest() {
      persistence = false;
   }

   // Expected invocations; many-commands always execute everywhere
   // Mode                    | O | P | B | Sum
   // Non-tx orig == owner    | 1 | = | 1 | 2
   // Non-tx orig != owner    | 0 | 1 | 1 | 2
   // Non-tx orig == owner RO | 1 | = | 0 | 1
   // Non-tx orig != owner RO | 0 | 1 | 0 | 1
   // TX orig == owner RO     | 1 | = | 0 | 1
   // TX orig == owner RW     | 1 | = | 1 | 2
   // TX orig == owner WO     | 1 | = | 1 | 2
   // TX orig == owner WOnoop | 1 | = | 0 | 1
   // TX orig != owner RO     | 0 | 1 | 0 | 1
   // TX orig != owner RW     | 0 | 1 | 1 | 2
   // TX orig != owner WO     | 1 | 1 | 1 | 3 // The write-only command creates the value in context
   // TX orig != owner WOnoop | 1 | = | 0 | 1 // The write-only command creates the value in context
   @Test(dataProvider = "owningModeAndWriteMethod")
   public void testWriteLoad(boolean isOwner, WriteMethod method) {
      Object key = getKey(isOwner, DIST);

      method.eval(key, wo, rw,
            view -> {
               assertFalse(view.find().isPresent());
               return null;
            },
            (view, nil) -> view.set("value"), getClass());

      assertInvocations(Boolean.TRUE.equals(transactional) && !isOwner && !method.doesRead ? 3 : 2);

      caches(DIST).forEach(cache -> assertEquals(cache.get(key), "value", getAddress(cache).toString()));
      caches(DIST).forEach(cache -> {
         if (cache.getAdvancedCache().getDistributionManager().getCacheTopology().isReadOwner(key)) {
            assertTrue(cacheContainsKey(key, cache), getAddress(cache).toString());
         } else {
            assertFalse(cacheContainsKey(key, cache), getAddress(cache).toString());
         }
      });

      resetInvocationCount();

      // this does not write anything
      method.eval(key, wo, rw,
            view -> {
               assertTrue(view.find().isPresent());
               assertEquals(view.get(), "value");
               return null;
            },
            (view, nil) -> {
            }, getClass());

      // TODO ISPN-8676: routing optimization for no-write many commands not implemented
      if (method.isMany) {
         assertInvocations(Boolean.TRUE.equals(transactional) && !isOwner && !method.doesRead ? 3 : 2);
      } else {
         assertInvocations(Boolean.TRUE.equals(transactional) && !isOwner && method.doesRead ? 2 : 1);
      }
   }

   @Test(dataProvider = "writeMethods")
   public void testWriteLoadLocal(WriteMethod method) {
      testWriteLoad(method, 1, lwo, lrw, null);
   }

   @Test(dataProvider = "writeMethods")
   public void testWriteLoadSimple(WriteMethod method) {
      checkSimpleCacheAvailable();
      testWriteLoad(method, 1, swo, srw, SIMPLE);
   }

   private <K, R> void testWriteLoad(WriteMethod method, K key, FunctionalMap.WriteOnlyMap<K, String> wo,
                                     FunctionalMap.ReadWriteMap<K, String> rw, String name) {
      method.eval(key, wo, rw,
            view -> {
               assertFalse(view.find().isPresent());
               return null;
            },
            (view, nil) -> view.set("value"), getClass());

      assertInvocations(1);
      Cache<K, String> c = name == null ? cacheManagers.get(0).getCache() : cacheManagers.get(0).getCache(name);
      assertEquals(c.get(key), "value");

      method.eval(key, wo, rw,
            view -> {
               assertTrue(view.find().isPresent());
               assertEquals(view.get(), "value");
               return null;
            },
            (view, nil) -> {
            }, getClass());

      assertInvocations(2);
   }

   @Test(dataProvider = "owningModeAndWriteMethod")
   public void testExceptionPropagation(boolean isOwner, WriteMethod method) {
      Object key = getKey(isOwner, DIST);
      try {
         method.eval(key, wo, rw,
               view -> null,
               (view, nil) -> {
                  throw new TestException();
               }, getClass());
         fail("Should throw CompletionException:CacheException:[RemoteException:]*TestException");
      } catch (CacheException | CompletionException e) { // catches RemoteExceptions, too
         Throwable t = e;
         if (Boolean.TRUE.equals(transactional) && t.getCause() instanceof RollbackException) {
            Throwable[] suppressed = t.getCause().getSuppressed();
            if (suppressed != null && suppressed.length > 0) {
               t = suppressed[0];
               assertEquals(XAException.class, t.getClass());
               t = t.getCause();
            }
         }
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
      Object key = getKey(isOwner, DIST);
      try {
         method.eval(key, null, rw,
               view -> view.get(),
               (view, nil) -> {
               }, getClass());
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
      Object key = getKey(isOwner, DIST);

      assertTrue(method.eval(key, ro, view -> {
         assertFalse(view.find().isPresent());
         return true;
      }));

      // we can't add from read-only cache, so we put manually:
      cache(0, DIST).put(key, "value");

      caches(DIST).forEach(cache -> assertEquals(cache.get(key), "value", getAddress(cache).toString()));
      caches(DIST).forEach(cache -> {
         if (cache.getAdvancedCache().getDistributionManager().getCacheTopology().isReadOwner(key)) {
            assertTrue(cacheContainsKey(key, cache), getAddress(cache).toString());
         } else {
            assertFalse(cacheContainsKey(key, cache), getAddress(cache).toString());
         }
      });

      assertEquals(method.eval(key, ro,
            view -> {
               assertTrue(view.find().isPresent());
               assertEquals(view.get(), "value");
               return "OK";
            }), "OK");
   }

   @Test(dataProvider = "readMethods")
   public void testReadLoadLocal(ReadMethod method) {
      Integer key = 1;

      assertTrue(method.eval(key, lro,
            view -> {
               assertFalse(view.find().isPresent());
               return true;
            }));

      // we can't add from read-only cache, so we put manually:
      Cache<Integer, String> cache = cacheManagers.get(0).getCache();
      cache.put(key, "value");

      assertEquals(cache.get(key), "value");

      assertEquals(method.eval(key, lro,
            view -> {
               assertTrue(view.find().isPresent());
               assertEquals(view.get(), "value");
               return "OK";
            }), "OK");
   }

   @Test(dataProvider = "owningModeAndReadMethod")
   public void testReadOnMissingValue(boolean isOwner, ReadMethod method) {
      testReadOnMissingValue(getKey(isOwner, DIST), ro, method);
   }

   @Test(dataProvider = "readMethods")
   public void testOnMissingValueLocal(ReadMethod method) {
      testReadOnMissingValue(0, ReadOnlyMapImpl.create(fmapL1), method);
   }

   protected boolean cacheContainsKey(Object key, Cache<Object, Object> cache) {
      return cache.getAdvancedCache().getDataContainer().containsKey(key);
   }
}
