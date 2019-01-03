package org.infinispan.persistence;

import static org.testng.AssertJUnit.assertEquals;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.LongStream;

import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.filter.KeyFilter;
import org.infinispan.marshall.persistence.impl.MarshalledEntryUtil;
import org.infinispan.persistence.spi.AdvancedCacheLoader;
import org.infinispan.persistence.spi.InitializationContext;
import org.infinispan.marshall.core.MarshalledEntry;
import org.infinispan.test.AbstractInfinispanTest;
import org.reactivestreams.Publisher;
import org.testng.annotations.Test;

import io.reactivex.Flowable;

/**
 * Test to verify some advanced cache loader methods work properly
 * @author wburns
 * @since 10.0
 */
@Test(groups = "functional", testName = "persistence.AdvancedCacheLoaderFunctionalTest")
public class AdvancedCacheLoaderFunctionalTest extends AbstractInfinispanTest {
   public void testProcessMethodImplemented() throws InterruptedException, ExecutionException, TimeoutException {
      long amount = 10_000;
      AdvancedCacheLoader acl = new AdvancedCacheLoader() {
         @Override
         public int size() {
            return 0;
         }

         @Override
         public void init(InitializationContext ctx) {
         }

         @Override
         public MarshalledEntry load(Object key) {
            return null;
         }

         @Override
         public boolean contains(Object key) {
            return false;
         }

         @Override
         public void start() {

         }

         @Override
         public void stop() {

         }

         @Override
         public void process(KeyFilter filter, CacheLoaderTask task, Executor executor, boolean fetchValue, boolean fetchMetadata) {
            TaskContext taskContext = new TaskContextImpl();
            LongStream.range(0, amount).forEach(i -> {
               try {
                  task.processEntry(MarshalledEntryUtil.create(i, i, (Marshaller) null), taskContext);
               } catch (InterruptedException e) {
                  e.printStackTrace();
               }
            });
         }
      };

      Publisher<Object> publisher = acl.publishEntries(null, true, true);

      Future<Long> future = fork(() -> Flowable.fromPublisher(publisher).count().blockingGet());
      Long value = future.get(10, TimeUnit.SECONDS);

      assertEquals(amount, value.longValue());
   }
}
