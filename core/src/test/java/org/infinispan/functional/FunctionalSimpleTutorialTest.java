package org.infinispan.functional;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.infinispan.AdvancedCache;
import org.infinispan.functional.EntryView.ReadEntryView;
import org.infinispan.functional.FunctionalMap.ReadOnlyMap;
import org.infinispan.functional.FunctionalMap.ReadWriteMap;
import org.infinispan.functional.FunctionalMap.WriteOnlyMap;
import org.infinispan.functional.MetaParam.MetaLifespan;
import org.infinispan.functional.impl.FunctionalMapImpl;
import org.infinispan.functional.impl.ReadOnlyMapImpl;
import org.infinispan.functional.impl.ReadWriteMapImpl;
import org.infinispan.functional.impl.WriteOnlyMapImpl;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "functional.FunctionalSimpleTutorialTest")
public class FunctionalSimpleTutorialTest extends SingleCacheManagerTest {

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      return TestCacheManagerFactory.createCacheManager();
   }

   public void testSimpleTutorial() throws Exception {
      AdvancedCache<String, String> cache = cacheManager.<String, String>getCache().getAdvancedCache();
      FunctionalMapImpl<String, String> functionalMap = FunctionalMapImpl.create(cache);
      WriteOnlyMap<String, String> writeOnlyMap = WriteOnlyMapImpl.create(functionalMap);
      ReadOnlyMap<String, String> readOnlyMap = ReadOnlyMapImpl.create(functionalMap);

      // Execute two parallel write-only operation to store key/value pairs
      CompletableFuture<Void> writeFuture1 = writeOnlyMap.eval("key1", "value1",
         (v, writeView) -> writeView.set(v));
      CompletableFuture<Void> writeFuture2 = writeOnlyMap.eval("key2", "value2",
         (v, writeView) -> writeView.set(v));

      // When each write-only operation completes, execute a read-only operation to retrieve the value
      CompletableFuture<String> readFuture1 =
         writeFuture1.thenCompose(r -> readOnlyMap.eval("key1", ReadEntryView::get));
      CompletableFuture<String> readFuture2 =
         writeFuture2.thenCompose(r -> readOnlyMap.eval("key2", ReadEntryView::get));

      // When the read-only operation completes, print it out
      printf("Created entries: %n");
      CompletableFuture<Void> end = readFuture1.thenAcceptBoth(readFuture2, (v1, v2) ->
         printf("key1 = %s%nkey2 = %s%n", v1, v2));

      // Wait for this read/write combination to finish
      end.get();

      // Create a read-write map
      ReadWriteMap<String, String> readWriteMap = ReadWriteMapImpl.create(functionalMap);

      // Use read-write multi-key based operation to write new values
      // together with lifespan and return previous values
      Map<String, String> data = new HashMap<>();
      data.put("key1", "newValue1");
      data.put("key2", "newValue2");
      Traversable<String> previousValues = readWriteMap.evalMany(data, (v, readWriteView) -> {
         String prev = readWriteView.find().orElse(null);
         readWriteView.set(v, new MetaLifespan(Duration.ofHours(1).toMillis()));
         return prev;
      });

      // Use read-only multi-key operation to read current values for multiple keys
      Traversable<ReadEntryView<String, String>> entryViews =
            readOnlyMap.evalMany(data.keySet(), readOnlyView -> readOnlyView);
      printf("Updated entries: %n");
      entryViews.forEach(view -> printf("%s%n", view));

      // Finally, print out the previous entry values
      printf("Previous entry values: %n");
      previousValues.forEach(prev -> printf("%s%n", prev));
   }

   private void printf(String format, Object ... args) {
      log.infof(format, args);
   }

}
