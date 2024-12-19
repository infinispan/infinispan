package org.infinispan.multimap.impl;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.commons.util.Util;
import org.infinispan.marshall.protostream.impl.MarshallableUserObject;
import org.infinispan.multimap.impl.internal.MultimapObjectWrapper;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Bucket used to store ListMultimap values.
 *
 * @author Katia Aresti
 * @since 15.0
 */
@ProtoTypeId(ProtoStreamTypeIds.MULTIMAP_LIST_BUCKET)
public class ListBucket<V> implements SortableBucket<V> {

   final Deque<V> values;

   public ListBucket() {
      this.values = new ArrayDeque<>(0);
   }

   public ListBucket(V value) {
      Deque<V> deque = new ArrayDeque<>(1);
      deque.add(value);
      this.values = deque;
   }

   private ListBucket(Deque<V> values) {
      this.values = values;
   }

   public static <V> ListBucket<V> create(Collection<V> value) {
      return new ListBucket<>(new ArrayDeque<V>(value));
   }

   @ProtoFactory
   ListBucket(Collection<MarshallableUserObject<V>> wrappedValues) {
      this((Deque<V>) wrappedValues.stream().map(MarshallableUserObject::get)
            .collect(Collectors.toCollection(ArrayDeque::new)));
   }

   @ProtoField(number = 1, collectionImplementation = ArrayList.class)
   Collection<MarshallableUserObject<V>> getWrappedValues() {
      return this.values.stream().map(MarshallableUserObject::new).collect(Collectors.toCollection(ArrayDeque::new));
   }

   public boolean contains(V value) {
      for (V v : values) {
         if (Objects.deepEquals(v, value)) {
            return Boolean.TRUE;
         }
      }
      return Boolean.FALSE;
   }

   public boolean isEmpty() {
      return values.isEmpty();
   }

   public long size() {
      return values.size();
   }

   /**
    * @return a defensive copy of the {@link #values} collection.
    */
   public Deque<V> toDeque() {
      return new ArrayDeque<>(values);
   }

   @Override
   public String toString() {
      return "ListBucket{values=" + Util.toStr(values) + '}';
   }

   public ListBucket<V> offer(Collection<V> value, boolean first) {
      Deque<V> newItems = new ArrayDeque<>(values);
      if (first) {
         for (V v : value) {
            newItems.offerFirst(v);
         }
      } else {
         for (V v : value) {
            newItems.offerLast(v);
         }
      }
      return new ListBucket<>(newItems);
   }

   public ListBucket<V> set(long index, V value) {
      if ((index >= 0 && (values.size() - 1 < index)) || (index < 0 && (values.size() + index < 0))) {
         return null;
      }

      ArrayDeque<V> newBucket = new ArrayDeque<>(values.size());
      if (index >= 0) {
         Iterator<V> ite = values.iterator();
         int currentIndex = 0;
         while (ite.hasNext()) {
            V element = ite.next();
            if (index == currentIndex) {
               newBucket.offerLast(value);
            } else {
               newBucket.offerLast(element);
            }
            currentIndex++;
         }
      } else {
         Iterator<V> ite = values.descendingIterator();
         int currentIndex = -1;
         while (ite.hasNext()) {
            V element = ite.next();
            if (index == currentIndex) {
               newBucket.offerFirst(value);
            } else {
               newBucket.offerFirst(element);
            }
            currentIndex--;
         }
      }
      return new ListBucket<>(newBucket);
   }

   public Collection<V> sublist(long from, long to) {
      // from and to are + but from is bigger
      // example: from 2 > to 1 -> empty result
      // from and to are - and to is smaller
      // example: from -1 > to -2 -> empty result
      if ((from > 0 && to > 0 && from > to) || (from < 0 && to < 0 && from > to)) {
         return Collections.emptyList();
      }

      // index request
      if (from == to) {
         V element = index(from);
         if (element != null) {
            return Collections.singletonList(element);
         }
         return Collections.emptyList();
      }

      List<V> result = new ArrayList<>();
      long fromIte = from < 0 ? values.size() + from : from;
      long toIte = to < 0 ? values.size() + to : to;

      Iterator<V> ite = values.iterator();
      int offset = 0;
      while (ite.hasNext()) {
         V element = ite.next();
         if (offset < fromIte) {
            offset++;
            continue;
         }
         if (offset > toIte) {
            break;
         }

         result.add(element);
         offset++;
      }
      return result;
   }

   public ListBucket<V> trim(long from, long to) {
      // from and to are + but from is bigger
      // example: from 2 > to 1 -> empty
      // from and to are - and to is smaller
      // example: from -1 > to -2 -> empty
      if ((from > 0 && to > 0 && from > to) || (from < 0 && to < 0 && from > to)) {
         return new ListBucket<>();
      }

      // index request
      if (from == to) {
         V element = index(from);
         return element != null
               ? new ListBucket<>(element)
               : this;
      }

      long startRemoveCount = from < 0 ? values.size() + from : from;
      long keepCount = (to < 0 ? values.size() + to : to) - startRemoveCount;

      Deque<V> newValues = new ArrayDeque<>(values);
      Iterator<V> ite = newValues.iterator();
      while (ite.hasNext() && startRemoveCount > 0) {
         ite.next();
         ite.remove();
         startRemoveCount--;
      }

      // keep elements
      while (ite.hasNext() && keepCount >= 0) {
         ite.next();
         keepCount--;
      }

      // remove remaining elements
      while (ite.hasNext()) {
         ite.next();
         ite.remove();
      }

      return new ListBucket<>(newValues);
   }

   public Collection<Long> indexOf(V element, long count, long rank, long maxLen) {
      long matches = count == 0 ? values.size() : count;
      long rankCount = Math.abs(rank);
      long comparisons = maxLen == 0 ? values.size() : maxLen;

      Iterator<V> ite;
      if (rank > 0) {
         ite = values.iterator();
      } else {
         ite = values.descendingIterator();
      }

      long pos = 0;
      List<Long> positions = new ArrayList<>();
      while (ite.hasNext() && comparisons > 0 && matches > 0) {
         V current = ite.next();
         if (Objects.deepEquals(element, current)) {
            if (rankCount == 1) {
               matches--;
               if (rank < 0) {
                  positions.add(values.size() - pos - 1);
               } else {
                  positions.add(pos);
               }
            } else {
               rankCount--;
            }
         }
         comparisons--;
         pos++;
      }
      return positions;
   }

   public ListBucket<V> insert(boolean before, V pivot, V element) {
      Deque<V> newValues = new ArrayDeque<>(values.size() + 1);

      Iterator<V> iterator = values.iterator();
      boolean found = false;
      while (iterator.hasNext()) {
         V next = iterator.next();
         if (found) {
            newValues.offerLast(next);
         } else {
            if (Objects.deepEquals(pivot, next)) {
               found = true;
               if (before) {
                  newValues.offerLast(element);
                  newValues.offerLast(next);
               } else {
                  newValues.offerLast(next);
                  newValues.offerLast(element);
               }
            } else {
               newValues.offerLast(next);
            }
         }
      }
      if (!found) {
         return null;
      }
      return new ListBucket<>(newValues);
   }

   public ListBucketResult<Long, V> remove(long count, V element) {
      Iterator<V> ite;
      if (count < 0) {
         ite = values.descendingIterator();
      } else {
         ite = values.iterator();
      }

      long maxRemovalsCount = count == 0 ? values.size() : Math.abs(count);
      long removedElements = 0L;
      Deque<V> newItems = new ArrayDeque<>();
      while (ite.hasNext()) {
         V next = ite.next();
         if (Objects.deepEquals(next, element) && removedElements < maxRemovalsCount) {
            removedElements++;
            continue;
         }

         if (count < 0) {
            newItems.addFirst(next);
         } else {
            newItems.addLast(next);
         }
      }

      return new ListBucketResult<>(removedElements, new ListBucket<>(newItems));
   }

   public ListBucketResult<V, V> rotate(boolean rotateRight) {
      Deque<V> newItems = new ArrayDeque<>(values);
      V element;
      if (rotateRight) {
         // from head to tail
         element = newItems.pollFirst();
         newItems.offerLast(element);
      } else {
         // from tail to head
         element = newItems.pollLast();
         newItems.offerFirst(element);
      }
      return new ListBucketResult<>(element, new ListBucket<>(newItems));
   }

   @Override
   public Stream<MultimapObjectWrapper<V>> stream() {
      return values.stream()
            .map(MultimapObjectWrapper::new);
   }

   @Override
   public List<ScoredValue<V>> sort(SortOptions sortOptions) {
      Stream<ScoredValue<V>> scoredValueStream;
      if (sortOptions.alpha) {
         scoredValueStream = values.stream().map(v -> {
            MultimapObjectWrapper<V> wrapped = new MultimapObjectWrapper<>(v);
            return new ScoredValue<>(1d, wrapped);
         });
      } else {
         scoredValueStream = values.stream().map(v -> {
            MultimapObjectWrapper<V> wrapped = new MultimapObjectWrapper<>(v);
            return new ScoredValue<>(wrapped.asDouble(), wrapped);
         });
      }
      return sort(scoredValueStream, sortOptions);
   }

   public ListBucket<V> replace(Deque<V> list) {
      if (list != null && !list.isEmpty()) {
         return new ListBucket<>(list);
      }

      return new ListBucket<>();
   }

   public record ListBucketResult<R, E>(R result, ListBucket<E> bucket) { }

   public ListBucketResult<Collection<V>, V> poll(boolean first, long count) {
      List<V> polledValues = new ArrayList<>();
      if (count >= values.size()) {
         if (first) {
            polledValues.addAll(values);
         } else {
            Iterator<V> ite = values.descendingIterator();
            while (ite.hasNext()) {
               polledValues.add(ite.next());
            }
         }
         return new ListBucketResult<>(polledValues, new ListBucket<>());
      }

      Deque<V> newItems = new ArrayDeque<>(values);
      for (int i = 0; i < count; i++) {
         if (first) {
            polledValues.add(newItems.pollFirst());
         } else {
            polledValues.add(newItems.pollLast());
         }
      }
      return new ListBucketResult<>(polledValues, new ListBucket<>(newItems));
   }

   public V index(long index) {
      if (index == 0) {
         return values.element();
      }
      if (index == values.size() - 1 || index == -1) {
         return values.getLast();
      }
      V result = null;
      if (index > 0) {
         if (index >= values.size()) {
            return null;
         }

         Iterator<V> iterator = values.iterator();
         int currentIndex = 0;
         while (currentIndex++ <= index) {
            result = iterator.next();
         }
      } else {
         long currentIndex = Math.abs(index);
         if (currentIndex > values.size()) {
            return null;
         }

         Iterator<V> iterator = values.descendingIterator();
         while (currentIndex-- > 0) {
            result = iterator.next();
         }
      }

      return result;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      try {
         ListBucket<V> that = (ListBucket<V>) o;
         if (values.size() != that.values.size()) return false;

         for (V value : values) {
            if (!that.contains(value))
               return false;
         }
      } catch (ClassCastException ignore) {
         return false;
      }


      return true;
   }

   @Override
   public int hashCode() {
      return Objects.hash(values);
   }
}
