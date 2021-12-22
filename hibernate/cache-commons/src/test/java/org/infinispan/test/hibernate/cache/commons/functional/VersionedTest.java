package org.infinispan.test.hibernate.cache.commons.functional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

import org.hibernate.ObjectNotFoundException;
import org.hibernate.PessimisticLockException;
import org.hibernate.Session;
import org.hibernate.StaleStateException;
import org.hibernate.cache.spi.access.AccessType;
import org.infinispan.AdvancedCache;
import org.infinispan.commands.functional.ReadWriteKeyCommand;
import org.infinispan.commons.util.ByRef;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.hibernate.cache.commons.access.SessionAccess;
import org.infinispan.hibernate.cache.commons.util.Caches;
import org.infinispan.hibernate.cache.commons.util.VersionedEntry;
import org.infinispan.interceptors.AsyncInterceptorChain;
import org.infinispan.interceptors.DDAsyncInterceptor;
import org.infinispan.interceptors.impl.CallInterceptor;
import org.infinispan.test.hibernate.cache.commons.functional.entities.Item;
import org.infinispan.test.hibernate.cache.commons.functional.entities.OtherItem;
import org.junit.Test;

import jakarta.transaction.Synchronization;


/**
 * Tests specific to versioned entries -based caches.
 * Similar to {@link TombstoneTest} but some cases have been removed since
 * we are modifying the cache only once, therefore some sequences of operations
 * would fail before touching the cache.
 *
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
public class VersionedTest extends AbstractNonInvalidationTest {

   protected static final SessionAccess SESSION_ACCESS = SessionAccess.findSessionAccess();

   @Override
   public List<Object[]> getParameters() {
      return Arrays.asList(NONSTRICT_REPLICATED, NONSTRICT_DISTRIBUTED, NONSTRICT_REPLICATED_STATS, NONSTRICT_DISTRIBUTED_STATS);
   }

   @Override
   protected boolean getUseQueryCache() {
      return false;
   }

   @Test
   public void testTwoRemoves() throws Exception {
      CyclicBarrier loadBarrier = new CyclicBarrier(2);
      CountDownLatch flushLatch = new CountDownLatch(2);
      CountDownLatch commitLatch = new CountDownLatch(1);

      Future<Boolean> first = removeFlushWait(itemId, loadBarrier, null, flushLatch, commitLatch);
      Future<Boolean> second = removeFlushWait(itemId, loadBarrier, null, flushLatch, commitLatch);
      awaitOrThrow(flushLatch);

      assertSingleEmpty();

      commitLatch.countDown();
      boolean firstResult = first.get(WAIT_TIMEOUT, TimeUnit.SECONDS);
      boolean secondResult = second.get(WAIT_TIMEOUT, TimeUnit.SECONDS);
      assertTrue(firstResult != secondResult);

      assertSingleEmpty();

      TIME_SERVICE.advance(timeout + 1);
      assertEmptyCache();
   }

   @Test
   public void testRemoveRolledBack() throws Exception {
      withTxSession(s -> {
         Item item = s.load(Item.class, itemId);
         s.delete(item);
         assertSingleCacheEntry();
         s.flush();

         markRollbackOnly(s);
      });
      assertSingleEmpty();
   }

   @Test
   public void testUpdateRolledBack() throws Exception {
      ByRef<Object> entryRef = new ByRef<>(null);
      withTxSession(s -> {
         Item item = s.load(Item.class, itemId);
         item.getDescription();
         Object prevEntry = assertSingleCacheEntry();
         entryRef.set(prevEntry);
         item.setDescription("Updated item");
         s.update(item);
         assertEquals(prevEntry, assertSingleCacheEntry());
         s.flush();
         assertEquals(prevEntry, assertSingleCacheEntry());
         markRollbackOnly(s);
      });
      assertEquals(entryRef.get(), assertSingleCacheEntry());
   }

   @Test
   public void testStaleReadDuringUpdate() throws Exception {
      ByRef<Object> entryRef = testStaleRead((s, item) -> {
         item.setDescription("Updated item");
         s.update(item);
      });
      assertNotEquals(entryRef.get(), assertSingleCacheEntry());
      withTxSession(s -> {
         Item item = s.load(Item.class, itemId);
         assertEquals("Updated item", item.getDescription());
      });
   }

   @Test
   public void testStaleReadDuringRemove() throws Exception {
      try {
         testStaleRead((s, item) -> s.delete(item));
         if (accessType == AccessType.NONSTRICT_READ_WRITE) {
            // Nonstrict removes preemptively to prevent permanent cache inconsistency
            fail("Should have thrown an ObjectNotFoundException!");
         }
      } catch (ObjectNotFoundException e) {
         if (accessType != AccessType.NONSTRICT_READ_WRITE) {
            throw e;
         }
      }
      assertSingleEmpty();
      withTxSession(s -> {
         Item item = s.get(Item.class, itemId);
         assertNull(item);
      });
   }

   protected ByRef<Object> testStaleRead(BiConsumer<Session, Item> consumer) throws Exception {
      AtomicReference<Exception> synchronizationException = new AtomicReference<>();
      CountDownLatch syncLatch = new CountDownLatch(1);
      CountDownLatch commitLatch = new CountDownLatch(1);

      Future<Boolean> action = executor.submit(() -> withTxSessionApply(s -> {
         try {
            SESSION_ACCESS.getTransactionCoordinator(s).registerLocalSynchronization(new Synchronization() {
               @Override
               public void beforeCompletion() {
               }

               @Override
               public void afterCompletion(int i) {
                  syncLatch.countDown();
                  try {
                     awaitOrThrow(commitLatch);
                  } catch (Exception e) {
                     synchronizationException.set(e);
                  }
               }
            });
            Item item = s.load(Item.class, itemId);
            consumer.accept(s, item);
            s.flush();
         } catch (StaleStateException e) {
            log.info("Exception thrown: ", e);
            markRollbackOnly(s);
            return false;
         } catch (PessimisticLockException e) {
            log.info("Exception thrown: ", e);
            markRollbackOnly(s);
            return false;
         }
         return true;
      }));
      awaitOrThrow(syncLatch);
      ByRef<Object> entryRef = new ByRef<>(null);
      try {
         withTxSession(s -> {
            Item item = s.load(Item.class, itemId);
            assertEquals("Original item", item.getDescription());
            entryRef.set(assertSingleCacheEntry());
         });
      } finally {
         commitLatch.countDown();
      }
      assertTrue(action.get(WAIT_TIMEOUT, TimeUnit.SECONDS));
      assertNull(synchronizationException.get());
      return entryRef;
   }

   @Test
   public void testUpdateEvictExpiration() throws Exception {
      CyclicBarrier loadBarrier = new CyclicBarrier(2);
      CountDownLatch preEvictLatch = new CountDownLatch(1);
      CountDownLatch postEvictLatch = new CountDownLatch(1);
      CountDownLatch flushLatch = new CountDownLatch(1);
      CountDownLatch commitLatch = new CountDownLatch(1);

      Future<Boolean> first = updateFlushWait(itemId, loadBarrier, null, flushLatch, commitLatch);
      Future<Boolean> second = evictWait(itemId, loadBarrier, preEvictLatch, postEvictLatch);
      awaitOrThrow(flushLatch);

      assertSingleCacheEntry();

      preEvictLatch.countDown();
      awaitOrThrow(postEvictLatch);
      assertSingleEmpty();

      commitLatch.countDown();
      first.get(WAIT_TIMEOUT, TimeUnit.SECONDS);
      second.get(WAIT_TIMEOUT, TimeUnit.SECONDS);

      assertSingleEmpty();

      TIME_SERVICE.advance(timeout + 1);
      assertEmptyCache();
   }

   @Test
   public void testEvictUpdateExpiration() throws Exception {
      // since the timestamp for update is based on session open/tx begin time, we have to do this sequentially
      sessionFactory().getCache().evictEntityData(Item.class, itemId);
      assertSingleEmpty();
      TIME_SERVICE.advance(1);

      withTxSession(s -> {
         Item item = s.load(Item.class, itemId);
         item.setDescription("Updated item");
         s.update(item);
      });

      assertSingleCacheEntry();
      TIME_SERVICE.advance(timeout + 1);
      assertSingleCacheEntry();
   }

   @Test
   public void testEvictAndPutFromLoad() throws Exception {
      sessionFactory().getCache().evictEntityData(Item.class, itemId);
      assertSingleEmpty();
      TIME_SERVICE.advance(1);

      withTxSession(s -> {
         Item item = s.load(Item.class, itemId);
         assertEquals("Original item", item.getDescription());
      });

      assertSingleCacheEntry();
      TIME_SERVICE.advance(TIMEOUT + 1);
      assertSingleCacheEntry();
   }

   @Test
   public void testCollectionUpdate() throws Exception {
      // the first insert puts VersionedEntry(null, null, timestamp), so we have to wait a while to cache the entry
      TIME_SERVICE.advance(1);

      withTxSession(s -> {
         Item item = s.load(Item.class, itemId);
         OtherItem otherItem = new OtherItem();
         otherItem.setName("Other 1");
         s.persist(otherItem);
         item.addOtherItem(otherItem);
      });
      withTxSession(s -> {
         Item item = s.load(Item.class, itemId);
         Set<OtherItem> otherItems = item.getOtherItems();
         assertFalse(otherItems.isEmpty());
         otherItems.remove(otherItems.iterator().next());
      });

      AdvancedCache collectionCache = TEST_SESSION_ACCESS.getRegion(sessionFactory(), Item.class.getName() + ".otherItems").getCache();
      CountDownLatch putFromLoadLatch = new CountDownLatch(1);
      AtomicBoolean committing = new AtomicBoolean(false);
      CollectionUpdateTestInterceptor collectionUpdateTestInterceptor = new CollectionUpdateTestInterceptor(putFromLoadLatch);
      AnotherCollectionUpdateTestInterceptor anotherInterceptor = new AnotherCollectionUpdateTestInterceptor(putFromLoadLatch, committing);
      AsyncInterceptorChain interceptorChain = collectionCache.getAsyncInterceptorChain();
      interceptorChain.addInterceptorBefore( collectionUpdateTestInterceptor, CallInterceptor.class );
      interceptorChain.addInterceptor(anotherInterceptor, 0);

      TIME_SERVICE.advance(1);
      Future<Boolean> addFuture = executor.submit(() -> withTxSessionApply(s -> {
         awaitOrThrow(collectionUpdateTestInterceptor.updateLatch);
         Item item = s.load(Item.class, itemId);
         OtherItem otherItem = new OtherItem();
         otherItem.setName("Other 2");
         s.persist(otherItem);
         item.addOtherItem(otherItem);
         committing.set(true);
         return true;
      }));

      Future<Boolean> readFuture = executor.submit(() -> withTxSessionApply(s -> {
         Item item = s.load(Item.class, itemId);
         assertTrue(item.getOtherItems().isEmpty());
         return true;
      }));

      addFuture.get();
      readFuture.get();
      interceptorChain.removeInterceptor(CollectionUpdateTestInterceptor.class);
      interceptorChain.removeInterceptor(AnotherCollectionUpdateTestInterceptor.class);

      withTxSession(s -> assertFalse(s.load(Item.class, itemId).getOtherItems().isEmpty()));
   }

   class CollectionUpdateTestInterceptor extends DDAsyncInterceptor {
      final AtomicBoolean firstPutFromLoad = new AtomicBoolean(true);
      final CountDownLatch putFromLoadLatch;
      final CountDownLatch updateLatch = new CountDownLatch(1);

      public CollectionUpdateTestInterceptor(CountDownLatch putFromLoadLatch) {
         this.putFromLoadLatch = putFromLoadLatch;
      }

      @Override
      public Object visitReadWriteKeyCommand(InvocationContext ctx, ReadWriteKeyCommand command) throws Throwable {
         if (command.hasAnyFlag(FlagBitSets.ZERO_LOCK_ACQUISITION_TIMEOUT)) {
            if (firstPutFromLoad.compareAndSet(true, false)) {
               updateLatch.countDown();
               awaitOrThrow(putFromLoadLatch);
            }
         }
         return super.visitReadWriteKeyCommand(ctx, command);
      }
   }

   class AnotherCollectionUpdateTestInterceptor extends DDAsyncInterceptor {
      final CountDownLatch putFromLoadLatch;
      final AtomicBoolean committing;

      public AnotherCollectionUpdateTestInterceptor(CountDownLatch putFromLoadLatch, AtomicBoolean committing) {
         this.putFromLoadLatch = putFromLoadLatch;
         this.committing = committing;
      }

      @Override
      public Object visitReadWriteKeyCommand(InvocationContext ctx, ReadWriteKeyCommand command) throws Throwable {
         if (committing.get() && !command.hasAnyFlag(FlagBitSets.ZERO_LOCK_ACQUISITION_TIMEOUT)) {
            putFromLoadLatch.countDown();
         }
         return super.visitReadWriteKeyCommand(ctx, command);
      }
   }

   protected void assertSingleEmpty() {
      Map contents = Caches.entrySet(entityCache).toMap();
      Object value;
      assertEquals(1, contents.size());
      value = contents.get(itemId);
      assertEquals(VersionedEntry.class, value.getClass());
      assertNull(((VersionedEntry) value).getValue());
   }
}
