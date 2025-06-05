package org.infinispan.jcache;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertNull;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;

import org.infinispan.commons.time.TimeService;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.jcache.embedded.JCacheLoaderAdapter;
import org.infinispan.jcache.util.InMemoryJCacheLoader;
import org.infinispan.marshall.TestObjectStreamMarshaller;
import org.infinispan.marshall.persistence.impl.MarshalledEntryFactoryImpl;
import org.infinispan.persistence.DummyInitializationContext;
import org.infinispan.persistence.spi.InitializationContext;
import org.infinispan.persistence.spi.MarshallableEntry;
import org.infinispan.persistence.spi.MarshallableEntryFactory;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.util.EmbeddedTimeService;
import org.infinispan.util.concurrent.BlockingManager;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Unit tests for {@link JCacheLoaderAdapter}.
 *
 * @author Roman Chigvintsev
 */
@Test(groups = "unit", testName = "jcache.JCacheLoaderAdapterTest")
public class JCacheLoaderAdapterTest extends AbstractInfinispanTest {
   private static TestObjectStreamMarshaller marshaller;
   private static InitializationContext ctx;

   private JCacheLoaderAdapter<Integer, String> adapter;

   @BeforeClass
   public static void setUpClass() {
      TimeService timeService = new EmbeddedTimeService();
      marshaller = new TestObjectStreamMarshaller();
      MarshallableEntryFactory marshalledEntryFactory = new MarshalledEntryFactoryImpl(marshaller);
      ctx = new DummyInitializationContext() {
         @Override
         public TimeService getTimeService() {
            return timeService;
         }

         @Override
         public MarshallableEntryFactory getMarshallableEntryFactory() {
            return marshalledEntryFactory;
         }

         @Override
         public BlockingManager getBlockingManager() {
            BlockingManager mock = Mockito.mock(BlockingManager.class);
            when(mock.runBlocking(any(Runnable.class), anyString())).then((Answer<CompletionStage<Void>>) invocation -> {
               ((Runnable) invocation.getArguments()[0]).run();
               return CompletableFutures.completedNull();
            });
            when(mock.supplyBlocking(any(Supplier.class), anyString())).then((Answer<CompletionStage<Object>>) invocation -> CompletableFuture.completedFuture(((Supplier<Object>) invocation.getArguments()[0]).get()));
            return mock;
         }
      };
   }

   @AfterClass
   public static void tearDownClass() {
      marshaller.stop();
   }

   @BeforeMethod
   public void setUpMethod() {
      adapter = new JCacheLoaderAdapter<>();
      adapter.start(ctx);
      adapter.setCacheLoader(new InMemoryJCacheLoader<Integer, String>().store(1, "v1").store(2, "v2"));
   }

   public void testLoad() throws ExecutionException, InterruptedException {
      assertNull(adapter.load(0, 0).toCompletableFuture().get());

      MarshallableEntry v1Entry = adapter.load(0, 1).toCompletableFuture().get();

      assertNotNull(v1Entry);
      assertEquals(1, v1Entry.getKey());
      assertEquals("v1", v1Entry.getValue());

      MarshallableEntry v2Entry = adapter.load(0, 2).toCompletableFuture().get();

      assertNotNull(v2Entry);
      assertEquals(2, v2Entry.getKey());
      assertEquals("v2", v2Entry.getValue());
   }
}
