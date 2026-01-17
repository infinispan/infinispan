package org.infinispan.testing;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

public final class Combinations {

   private Combinations() { }

   /**
    * Generates all combinations (the power set) of the given enum values.
    *
    * @param clazz The enum class to generate combinations for.
    * @param <T>   The enum type.
    * @return A list of sets, where each set is a unique combination.
    */
   public static <T extends Enum<T>> List<Set<T>> combine(Class<T> clazz) {
      Objects.requireNonNull(clazz, "Enum class must not be null");
      if (!clazz.isEnum())
         throw new IllegalArgumentException("Provided class is not an enum: " + clazz.getName());

      List<Set<T>> combinations = new ArrayList<>();
      T[] constants = clazz.getEnumConstants();
      int n = constants.length;

      for (int i = 0; i < (1 << n); i++) {
         Set<T> curr = EnumSet.noneOf(clazz);
         for (int j = 0; j < n; j++) {
            if ((i & (1 << j)) != 0) {
               curr.add(constants[j]);
            }
         }
         combinations.add(curr);
      }

      return combinations;
   }
}
