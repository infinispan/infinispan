package org.infinispan.query.core.impl;

import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.StreamSupport;

import org.infinispan.commons.util.Closeables;
import org.testng.Assert;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "query.core.impl.MappingIteratorTest")
public class MappingIteratorTest {
   private final List<Integer> integers = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

   @Test
   public void testIteration() {
      MappingIterator<Integer, String> mapIterator = createMappingIterator(integers);
      assertIterator(mapIterator, "1", "2", "3", "4", "5", "6", "7", "8", "9", "10");

      mapIterator = createMappingIterator(integers).limit(0);
      assertIterator(mapIterator);

      mapIterator = createMappingIterator(integers).limit(2);
      assertIterator(mapIterator, "1", "2");

      mapIterator = createMappingIterator(integers).limit(20);
      assertIterator(mapIterator, "1", "2", "3", "4", "5", "6", "7", "8", "9", "10");

      mapIterator = createMappingIterator(integers).skip(4);
      assertIterator(mapIterator, "5", "6", "7", "8", "9", "10");

      mapIterator = createMappingIterator(integers).skip(11);
      assertIterator(mapIterator);

      mapIterator = createMappingIterator(integers).skip(0).limit(10);
      assertIterator(mapIterator, "1", "2", "3", "4", "5", "6", "7", "8", "9", "10");

      mapIterator = createMappingIterator(integers).skip(5).limit(2);
      assertIterator(mapIterator, "6", "7");

      mapIterator = createMappingIterator(integers).skip(1).limit(1);
      assertIterator(mapIterator, "2");

      mapIterator = createMappingIterator(integers).skip(50).limit(2);
      assertIterator(mapIterator);

      mapIterator = createMappingIterator(Arrays.asList(1, null, 3, null, 4));
      assertIterator(mapIterator, "1", "3", "4");

      mapIterator = createMappingIterator(Arrays.asList(1, null, 3, null, 4, 5, 6)).skip(1).limit(3);
      assertIterator(mapIterator, "3", "4", "5");
   }

   @Test
   public void testFiltering() {
      Function<Integer, String> EVEN = integer -> integer % 2 != 0 ? null : String.valueOf(integer);
      Function<Integer, String> LT5 = integer -> integer < 5 ? String.valueOf(integer) : null;
      Function<Integer, String> ALL = String::valueOf;
      Function<Integer, String> NONE = integer -> null;

      MappingIterator<Integer, String> mappingIterator = createMappingIterator(integers, EVEN).skip(1);
      assertIterator(mappingIterator, "4", "6", "8", "10");

      mappingIterator = createMappingIterator(integers, EVEN);
      assertIterator(mappingIterator, "2", "4", "6", "8", "10");

      mappingIterator = createMappingIterator(integers, EVEN).skip(1);
      assertIterator(mappingIterator, "4", "6", "8", "10");

      mappingIterator = createMappingIterator(integers, LT5);
      assertIterator(mappingIterator, "1", "2", "3", "4");

      mappingIterator = createMappingIterator(integers, ALL);
      assertIterator(mappingIterator, "1", "2", "3", "4", "5", "6", "7", "8", "9", "10");

      mappingIterator = createMappingIterator(integers, LT5).skip(1).limit(1);
      assertIterator(mappingIterator, "2");

      mappingIterator = createMappingIterator(integers, LT5).skip(5).limit(10);
      assertIterator(mappingIterator);

      mappingIterator = createMappingIterator(integers, NONE);
      assertIterator(mappingIterator);
   }

   private MappingIterator<Integer, String> createMappingIterator(List<Integer> data) {
      return new MappingIterator<>(Closeables.iterator(data.iterator()), String::valueOf);
   }

   private MappingIterator<Integer, String> createMappingIterator(List<Integer> data, Function<Integer, String> fn) {
      return new MappingIterator<>(Closeables.iterator(data.iterator()), fn);
   }

   private void assertIterator(MappingIterator<Integer, String> iterator, String... expected) {
      Iterable<String> iterable = () -> iterator;
      Object[] elements = StreamSupport.stream(iterable.spliterator(), false).toArray();
      Assert.assertEquals(elements, expected);
   }
}
