package org.infinispan.client.hotrod.impl.iteration;

import static org.testng.Assert.assertTrue;

import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.query.testdomain.protobuf.AccountPB;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.query.dsl.embedded.testdomain.hsearch.AccountHS;

/**
 * @author gfernandes
 * @since 8.0
 */
@SuppressWarnings("unchecked")
public interface AbstractRemoteIteratorTest {

   default <T> Set<T> setOf(T... values) {
      return Stream.of(values).collect(Collectors.toSet());
   }

   default <T> void populateCache(int numElements, Function<Integer, T> supplier, RemoteCache<Integer, T> remoteCache) {
      IntStream.range(0, numElements).parallel().forEach(i -> remoteCache.put(i, supplier.apply(i)));
   }

   default <T> void assertForAll(Collection<T> elements, Predicate<? super T> condition) {
      assertTrue(elements.stream().allMatch(condition));
   }

   default AccountHS newAccount(int id) {
      AccountHS account = new AccountHS();
      account.setId(id);
      account.setDescription("description for " + id);
      account.setCreationDate(new Date());
      return account;
   }

   default AccountPB newAccountPB(int id) {
      AccountPB account = new AccountPB();
      account.setId(id);
      account.setDescription("description for " + id);
      account.setCreationDate(new Date());
      return account;
   }

   default Set<Integer> rangeAsSet(int minimum, int maximum) {
      return IntStream.range(minimum, maximum).boxed().collect(Collectors.toSet());
   }

   default <K> Set<K> extractKeys(Collection<Map.Entry<Object, Object>> entries) {
      return entries.stream().map(e -> (K) e.getKey()).collect(Collectors.toSet());
   }

   default <V> Set<V> extractValues(Collection<Map.Entry<Object, Object>> entries) {
      return entries.stream().map(e -> (V) e.getValue()).collect(Collectors.toSet());
   }

   default byte[] toByteBuffer(Object key, Marshaller marshaller) {
      try {
         return marshaller.objectToByteBuffer(key);
      } catch (Exception ignored) { }
      return null;
   }

   default void assertKeysInSegment(Set<Map.Entry<Object, Object>> entries, Set<Integer> segments,
                                    Marshaller marshaller, Function<byte[], Integer> segmentCalculator) {
      entries.forEach(e -> {
         Integer key = (Integer) e.getKey();
         int segment = segmentCalculator.apply(toByteBuffer(key, marshaller));
         assertTrue(segments.contains(segment));
      });
   }

   default <K,V> Set<Map.Entry<K, V>> extractEntries(CloseableIterator<Map.Entry<Object, Object>> iterator) {
      Set<Map.Entry<K, V>> entries = new HashSet<>();
      try {
         while (iterator.hasNext()) {
            entries.add((Map.Entry<K, V>) iterator.next());
         }
      } finally {
         iterator.close();
      }
      return entries;
   }
}
