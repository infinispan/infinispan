package org.infinispan.commons.util;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collections;
import java.util.List;

import org.junit.Test;

public class ImmutableListCopyTest {

   @Test
   public void testImmutableFromCombined() {
      assertThat(new ImmutableListCopy<>(List.of(1, 2), List.of(3, 4)))
            .hasSize(4)
            .containsExactly(1, 2, 3, 4);

      assertThat(new ImmutableListCopy<>(List.of(1, 2), Collections.emptyList()))
            .hasSize(2)
            .containsExactly(1, 2);

      assertThat(new ImmutableListCopy<>(Collections.emptyList(), List.of(3, 4)))
            .hasSize(2)
            .containsExactly(3, 4);
   }

   @Test
   public void testIndexOf() {
      ImmutableListCopy<Integer> ilc = new ImmutableListCopy<>(List.of(1, 2, 42), List.of(42, 3, 4));

      assertThat(ilc.lastIndexOf(null)).isEqualTo(-1);
      assertThat(ilc.lastIndexOf(42)).isEqualTo(3);
      assertThat(ilc.indexOf(42)).isEqualTo(2);
      assertThat(ilc.indexOf(null)).isEqualTo(-1);
   }
}
