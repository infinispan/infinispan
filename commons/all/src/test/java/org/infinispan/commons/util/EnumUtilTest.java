package org.infinispan.commons.util;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collections;
import java.util.EnumSet;
import java.util.List;

import org.junit.Test;

public class EnumUtilTest {

   @Test
   public void testConversionOperations() {
      enum Test {
         A, B, C, D, E;
      }

      long ac = 0b0101;
      long ace = 0b10101;
      assertThat(EnumUtil.bitSetOf(Test.A, Test.C)).isEqualTo(ac);
      assertThat(EnumUtil.bitSetOf(Test.A, Test.C, Test.E)).isEqualTo(ace);
      assertThat(EnumUtil.bitSetOf(Collections.<Test>emptyList())).isEqualTo(0L);
      assertThat(EnumUtil.bitSetOf(List.of(Test.A, Test.C, Test.E))).isEqualTo(ace);
      assertThat(EnumUtil.bitSetOf(new Enum[] { Test.A, Test.C, Test.E })).isEqualTo(ace);
   }

   @Test
   public void testCreationOperations() {
      enum Test {
         A, B, C, D, E;
      }

      long ace = 0b10101;
      assertThat(EnumUtil.enumSetOf(ace, Test.class)).isEqualTo(EnumSet.of(Test.A, Test.C, Test.E));
      assertThat(EnumUtil.enumSetOf(0L, Test.class)).isEqualTo(EnumSet.noneOf(Test.class));
      assertThat(EnumUtil.hasEnum(ace, Test.A)).isTrue();
      assertThat(EnumUtil.hasEnum(ace, Test.B)).isFalse();

      assertThat(EnumUtil.setEnum(ace, Test.B)).isEqualTo(0b10111);
      assertThat(EnumUtil.setEnum(ace, Test.A)).isEqualTo(ace);

      assertThat(EnumUtil.unsetEnum(ace, Test.B)).isEqualTo(ace);
      assertThat(EnumUtil.unsetEnum(ace, Test.E)).isEqualTo(0b00101);
   }
}
