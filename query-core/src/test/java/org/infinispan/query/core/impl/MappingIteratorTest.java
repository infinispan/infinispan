package org.infinispan.query.core.impl;

import java.util.Arrays;
import java.util.List;
import java.util.stream.StreamSupport;

import org.infinispan.commons.util.Closeables;
import org.testng.Assert;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "query.core.tests.MappingIteratorTest")
public class MappingIteratorTest {

   @Test
   public void testIteration() {
      List<Integer> integers = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

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
   }

   private MappingIterator<Integer, String> createMappingIterator(List<Integer> data) {
      return new MappingIterator<>(Closeables.iterator(data.iterator()), String::valueOf);
   }

   private void assertIterator(MappingIterator<Integer, String> iterator, String... expected) {
      Iterable<String> iterable = () -> iterator;
      Object[] elements = StreamSupport.stream(iterable.spliterator(), false).toArray();
      Assert.assertEquals(elements, expected);
   }
}
