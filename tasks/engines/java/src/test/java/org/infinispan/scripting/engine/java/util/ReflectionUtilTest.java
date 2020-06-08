package org.infinispan.scripting.engine.java.util;

import static org.assertj.core.api.Assertions.assertThat;

import org.assertj.core.api.Assertions;
import org.junit.Test;

public class ReflectionUtilTest {
   @Test
   public void testMatchesArguments() {
      Assertions.assertThat(ReflectionUtil.matchesArguments(
            new Class<?>[]{int.class, double.class},
            new Object[]{123, 3.1416}))
            .isEqualTo(true);

      assertThat(ReflectionUtil.matchesArguments(
            new Class<?>[]{String.class, Integer.class},
            new Object[]{"Hello", 123}))
            .isEqualTo(true);

      assertThat(ReflectionUtil.matchesArguments(
            new Class<?>[]{Integer.class},
            new Object[]{"Hello"}))
            .isEqualTo(false);
   }

   @Test
   public void testMatchesArgumentsNull() {
      assertThat(ReflectionUtil.matchesArguments(
            new Class<?>[]{String.class},
            new Object[]{null}))
            .isEqualTo(true);

      assertThat(ReflectionUtil.matchesArguments(
            new Class<?>[]{int.class},
            new Object[]{null}))
            .isEqualTo(false);
   }

   @Test
   public void testMatchesType() {
      assertThat(ReflectionUtil.matchesType(int.class, int.class)).isEqualTo(true);
      assertThat(ReflectionUtil.matchesType(Integer.class, Integer.class)).isEqualTo(true);

      assertThat(ReflectionUtil.matchesType(int.class, Integer.class)).isEqualTo(true);
      assertThat(ReflectionUtil.matchesType(long.class, Long.class)).isEqualTo(true);
      assertThat(ReflectionUtil.matchesType(short.class, Short.class)).isEqualTo(true);
      assertThat(ReflectionUtil.matchesType(byte.class, Byte.class)).isEqualTo(true);
      assertThat(ReflectionUtil.matchesType(boolean.class, Boolean.class)).isEqualTo(true);
      assertThat(ReflectionUtil.matchesType(float.class, Float.class)).isEqualTo(true);
      assertThat(ReflectionUtil.matchesType(double.class, Double.class)).isEqualTo(true);
      assertThat(ReflectionUtil.matchesType(char.class, Character.class)).isEqualTo(true);

      assertThat(ReflectionUtil.matchesType(Object.class, Integer.class)).isEqualTo(true);

      assertThat(ReflectionUtil.matchesType(int.class, boolean.class)).isEqualTo(false);
      assertThat(ReflectionUtil.matchesType(Integer.class, String.class)).isEqualTo(false);
   }
}
