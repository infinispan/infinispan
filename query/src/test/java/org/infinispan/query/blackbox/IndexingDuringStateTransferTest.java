package org.infinispan.query.blackbox;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.infinispan.configuration.cache.IndexStorage.LOCAL_HEAP;
import static org.infinispan.query.helper.TestQueryHelperFactory.queryAll;
import static org.infinispan.test.TestingUtil.extractComponent;
import static org.infinispan.test.TestingUtil.orTimeout;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNull;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.configuration.internal.PrivateCacheConfigurationBuilder;
import org.infinispan.context.Flag;
import org.infinispan.query.helper.StaticTestingErrorHandler;
import org.infinispan.query.test.AnotherGrassEater;
import org.infinispan.query.test.Person;
import org.infinispan.query.test.QueryTestSCI;
import org.infinispan.reactive.publisher.impl.commands.batch.PublisherResponse;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.responses.ValidResponse;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.remoting.transport.impl.RequestRepository;
import org.infinispan.test.Mocks;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.topology.CacheTopology;
import org.infinispan.util.ControlledConsistentHashFactory;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "query.blackbox.IndexingDuringStateTransferTest")
public class IndexingDuringStateTransferTest extends MultipleCacheManagersTest {
   private static final String KEY = "k";
   private static final Person RADIM = new Person("Radim", "Tough guy!", 29);
   private static final Person DAN = new Person("Dan", "Not that tough.", 39);
   private static final AnotherGrassEater FLUFFY = new AnotherGrassEater("Fluffy", "Very cute.");

   private ConfigurationBuilder builder;
   private final ControlledConsistentHashFactory.Default chf = new ControlledConsistentHashFactory.Default(0, 1);

   @Override
   protected void createCacheManagers() {
      builder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
      builder.indexing()
            .enable()
            .storage(LOCAL_HEAP)
            .addIndexedEntity(Person.class)
            .addIndexedEntity(AnotherGrassEater.class);
      builder.clustering().hash().numSegments(1).numOwners(2);
      builder.addModule(PrivateCacheConfigurationBuilder.class).consistentHashFactory(chf);
      createClusteredCaches(2, globalConfigurationBuilder(), builder);
   }

   private GlobalConfigurationBuilder globalConfigurationBuilder() {
      GlobalConfigurationBuilder globalBuilder = defaultGlobalConfigurationBuilder();
      globalBuilder.serialization().addContextInitializers(QueryTestSCI.INSTANCE, ControlledConsistentHashFactory.SCI.INSTANCE);
      return globalBuilder;
   }

   @BeforeMethod
   public void cleanData() {
      caches().forEach(Cache::clear);
   }

   private RequestRepository spyRequestRepository(Cache<Object, Object> cache) {
      // This is assumed to be a JGroupsTransport
      Transport transport = extractComponent(cache, Transport.class);
      return Mocks.replaceFieldWithSpy(transport, "requests");
   }

   public void testPut() {
      test(c -> c.put(KEY, FLUFFY), this::assertFluffyIndexed);
   }

   public void testPutIgnoreReturnValue() {
      test(c -> c.getAdvancedCache().withFlags(Flag.IGNORE_RETURN_VALUES).put(KEY, FLUFFY), this::assertFluffyIndexed);
   }

   public void testPutMap() {
      test(c -> c.putAll(Collections.singletonMap(KEY, FLUFFY)), this::assertFluffyIndexed);
   }

   public void testReplace() {
      test(c -> c.replace(KEY, FLUFFY), this::assertFluffyIndexed);
   }

   public void testRemove() {
      test(c -> c.remove(KEY), sm -> {
      });
   }

   public void testCompute() {
      test(c -> c.compute(KEY, (k, old) -> FLUFFY), this::assertFluffyIndexed);
   }

   // The test fails as compute command on backup owner retrieves remote value from primary owner, but this may
   // already have the value applied. This command then does not commit (as it thinks that there is no change)
   // and therefore CommitManager does not know that it must not let the entry be state-transferred in.
   @Test(enabled = false, description = "ISPN-7590")
   public void testComputeRemove() {
      test(c -> c.compute(KEY, (k, old) -> null), sm -> {
      });
   }

   public void testMerge() {
      test(c -> c.merge(KEY, FLUFFY, (o, n) -> n), this::assertFluffyIndexed);
   }

   // Same as above, though this started failing only after functional commands were triangelized
   @Test(enabled = false, description = "ISPN-7590")
   public void testMergeRemove() {
      test(c -> c.merge(KEY, FLUFFY, (o, n) -> null), sm -> {
      });
   }

   /**
    * The test checks that when we replace a Person entity with entity of another type (Animal)
    * or completely remove it the old index is still correctly updated.
    */
   private void test(Consumer<Cache<Object, Object>> op, Consumer<Cache<Object, Object>> check) {
      Cache<Object, Object> cache0 = cache(0);
      Cache<Object, Object> cache1 = cache(1);

      assertEquals(0, queryAll(cache0, Person.class).size());

      // add a new key
      cache0.put(KEY, RADIM);

      // to prevent the case when index becomes empty (and we mistake it for correctly removed item) we add another person
      cache0.put("k2", DAN);

      StaticTestingErrorHandler.assertAllGood(cache0, cache1);

      List<Person> found = queryAll(cache0, Person.class);
      assertEquals(Arrays.asList(RADIM, DAN), sortByAge(found));

      // add new node, cache(0) will lose ownership of the segment
      chf.setOwnerIndexes(1, 2);
      addClusterEnabledCacheManager(globalConfigurationBuilder(), builder).getCache();

      // wait until the node discards old entries
      eventuallyEquals(null, () -> cache0.getAdvancedCache().getDataContainer().peek(KEY));

      // block state transfer responses
      AtomicReference<Throwable> exception = new AtomicReference<>();
      CompletableFuture<Void> allowStateResponse = new CompletableFuture<>();

      caches().forEach(c -> {
         RequestRepository spyRequests = spyRequestRepository(c);
         doAnswer(invocation -> {
            Response response = invocation.getArgument(2);
            if (response instanceof ValidResponse) {
               Object responseValue = ((ValidResponse) response).getResponseValue();
               if (responseValue instanceof PublisherResponse) {
                  CompletableFuture<Void> stageWithTimeout = orTimeout(allowStateResponse, 10, SECONDS,
                        testExecutor());
                  try {
                     stageWithTimeout.join();
                  } catch (Throwable t) {
                     exception.set(t);
                  }
               }
            }
            return invocation.callRealMethod();
         }).when(spyRequests).addResponse(anyLong(), any(), any());
      });

      // stop the other cache, cache(0) should get the entry back
      chf.setOwnerIndexes(0, 1);
      Future<?> stoppingCacheFuture = fork(() -> killMember(2));

      // wait until it starts rebalancing
      TestingUtil.waitForTopologyPhase(Collections.emptyList(), CacheTopology.Phase.READ_OLD_WRITE_ALL, cache(0));

      // check that cache(0) does not have the data yet
      assertNull(cache0.getAdvancedCache().getDataContainer().peek(KEY));

      // overwrite the entry with another type
      op.accept(cache0);

      // unblock state transfer
      allowStateResponse.complete(null);

      // wait for the new node to be added
      try {
         stoppingCacheFuture.get(10, SECONDS);
      } catch (Exception e) {
         throw new RuntimeException(e);
      }
      assertNull(exception.get());

      // check results
      StaticTestingErrorHandler.assertAllGood(cache0, cache(1));
      // The person should have been removed
      assertEquals(Collections.singletonList(DAN), queryAll(cache0, Person.class));
      Object value = cache(0).get(KEY);
      assertFalse("Current value: " + value, value instanceof Person);

      // Check that the operation is reflected in index
      check.accept(cache0);
   }

   private List<Person> sortByAge(List<Person> people) {
      people.sort(Comparator.comparingInt(Person::getAge));
      return people;
   }

   private void assertFluffyIndexed(Cache<Object,Object> cache) {
      assertEquals(Collections.singletonList(FLUFFY), queryAll(cache, AnotherGrassEater.class));
   }
}
