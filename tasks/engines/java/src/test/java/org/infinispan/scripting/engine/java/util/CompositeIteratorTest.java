package org.infinispan.scripting.engine.java.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Arrays;
import java.util.NoSuchElementException;

import org.junit.Test;

public class CompositeIteratorTest {
   @Test
   public void testEmpty() {
      CompositeIterator<Integer> iterator = new CompositeIterator<>();

      assertThat(iterator.hasNext()).isEqualTo(false);
      assertThat(iterator.hasNext()).isEqualTo(false);

      assertThatThrownBy(() -> {
         iterator.next();
      }).isInstanceOf(NoSuchElementException.class);
   }

   @Test
   public void testSingle() {
      CompositeIterator<Integer> iterator = new CompositeIterator<>(
            Arrays.asList(1, 2).iterator());

      assertThat(iterator.hasNext()).isEqualTo(true);
      assertThat(iterator.next()).isEqualTo(1);

      assertThat(iterator.hasNext()).isEqualTo(true);
      assertThat(iterator.next()).isEqualTo(2);

      assertThat(iterator.hasNext()).isEqualTo(false);

      assertThatThrownBy(() -> {
         iterator.next();
      }).isInstanceOf(NoSuchElementException.class);
   }


   @Test
   public void testMultiple() {
      CompositeIterator<Integer> iterator = new CompositeIterator<>(
            Arrays.asList(1, 2).iterator(),
            Arrays.asList(3, 4).iterator());

      assertThat(iterator.hasNext()).isEqualTo(true);
      assertThat(iterator.next()).isEqualTo(1);

      assertThat(iterator.hasNext()).isEqualTo(true);
      assertThat(iterator.next()).isEqualTo(2);

      assertThat(iterator.hasNext()).isEqualTo(true);
      assertThat(iterator.next()).isEqualTo(3);

      assertThat(iterator.hasNext()).isEqualTo(true);
      assertThat(iterator.next()).isEqualTo(4);

      assertThat(iterator.hasNext()).isEqualTo(false);

      assertThatThrownBy(() -> {
         iterator.next();
      }).isInstanceOf(NoSuchElementException.class);
   }
}
