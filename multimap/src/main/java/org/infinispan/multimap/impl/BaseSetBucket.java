package org.infinispan.multimap.impl;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;

import org.infinispan.multimap.impl.internal.MultimapObjectWrapper;

/**
 * Implements base functionalities shared by set-like data structures.
 *
 * <p>
 * Some functionalities in set are shared across sets. In such a way that it requires a common interface when operating
 * with the {@link org.infinispan.functional.FunctionalMap} API. This allows for functions to operate in any concrete
 * implementations of sets.
 * </p>
 *
 * @param <E> Type of elements in the set.
 */
public interface BaseSetBucket<E> {

   /**
    * Calculates the union of two sets.
    *
    * <p>
    * The method calculates the union of this {@link BaseSetBucket} instance with the provided input collection. In case of
    * duplicates, the elements in the input take precedence.
    * </p>
    *
    * @param input Items from another set to create the union.
    * @param weight Weight to apply to scores of each element.
    * @param function Aggregate function to apply to scores.
    * @return Collection with the elements of the union of the two sets.
    */
   default Collection<ScoredValue<E>> union(Collection<ScoredValue<E>> input, double weight, SortedSetBucket.AggregateFunction function) {
      SortedSet<ScoredValue<E>> output = new TreeSet<>();
      Map<MultimapObjectWrapper<E>, Double> merge = new HashMap<>();
      Iterator<ScoredValue<E>> ite;

      if (input != null) {
         ite = input.iterator();

         while (ite.hasNext()) {
            ScoredValue<E> element = ite.next();
            Double existing = getScore(element.wrappedValue());
            Double unionScore;
            if (existing == null) {
               unionScore = element.score();
            } else {
               unionScore = function.apply(element.score(), SetUtil.calculate(existing, weight));
            }
            output.add(new ScoredValue<>(unionScore, element.wrappedValue()));
            merge.put(element.wrappedValue(), unionScore);
         }
      }

      ite = getAsSet().iterator();
      while (ite.hasNext()) {
         ScoredValue<E> element = ite.next();
         Double existing = merge.get(element.wrappedValue());
         if (existing == null) {
            output.add(new ScoredValue<>(SetUtil.calculate(element.score(), weight), element.wrappedValue()));
         }
      }

      return output;
   }

   /**
    * Calculates the intersection of two sets.
    *
    * <p>
    * Calculates the intersection of this {@link BaseSetBucket} instance with the provided input collection.
    * </p>
    *
    * @param input Items from another set to create the intersection.
    * @param weight Weight to apply to scores of each element.
    * @param function Aggregate function to apply to scores.
    * @return Collection with the elements in the intersection of the two sets.
    */
   default Collection<ScoredValue<E>> inter(Collection<ScoredValue<E>> input, double weight, SortedSetBucket.AggregateFunction function) {
      if (input == null || input.isEmpty()) {
         return getAsSet().stream()
               .map(s -> new ScoredValue<>(SetUtil.calculate(s.score(), weight), s.wrappedValue()))
               .collect(Collectors.toList());
      }

      SortedSet<ScoredValue<E>> output = new TreeSet<>();
      for (ScoredValue<E> element : input) {
         Double existing = getScore(element.wrappedValue());
         if (existing != null) {
            double score = function.apply(element.score(), SetUtil.calculate(existing, weight));
            output.add(new ScoredValue<>(score, element.wrappedValue()));
         }
      }

      return output;
   }

   Set<ScoredValue<E>> getAsSet();

   List<ScoredValue<E>> getAsList();

   Double getScore(MultimapObjectWrapper<E> key);

   final class SetUtil {
      private SetUtil() { }


      private static double calculate(Double existing, double weight) {
         return weight == 0d ? 0d : existing * weight;
      }
   }
}
