package org.infinispan.multimap.impl;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.commons.util.Util;
import org.infinispan.marshall.protostream.impl.MarshallableUserObject;
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

/**
 * Bucket used to store ListMultimap values.
 *
 * @author Katia Aresti
 * @since 15.0
 */
@ProtoTypeId(ProtoStreamTypeIds.MULTIMAP_LIST_BUCKET)
public class ListBucket<V> {

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

   public static <V> ListBucket<V> create(V value) {
      return new ListBucket<>(value);
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

   public int size() {
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

   public ListBucket<V> offer(V value, boolean first) {
      if (first) {
         values.offerFirst(value);
      } else {
         values.offerLast(value);
      }

      return new ListBucket<>(values);
   }

   public ListBucket<V> set(long index, V value) {
      if ((index >= 0 && (values.size()-1 < index)) || (index < 0 && (values.size() + index < 0))) {
        return null;
      }

      // set head
      if (index == 0) {
         values.pollFirst();
         values.offerFirst(value);
         return new ListBucket<>(values);
      }

      // set tail
      if (index == -1 || index == values.size() -1) {
         values.pollLast();
         values.offerLast(value);
         return new ListBucket<>(values);
      }

      ArrayDeque<V> newBucket = new ArrayDeque<>(values.size());
      if (index > 0) {
         Iterator<V> ite = values.iterator();
         int currentIndex = 0;
         while(ite.hasNext()) {
            V element = ite.next();
            if (index == currentIndex) {
               newBucket.offerLast(value);
            } else {
               newBucket.offerLast(element);
            }
            currentIndex++;
         }
      }

      if (index < -1) {
         Iterator<V> ite = values.descendingIterator();
         int currentIndex = -1;
         while(ite.hasNext()) {
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
         if (offset < fromIte){
            offset++;
            continue;
         }
         if (offset > toIte){
            break;
         }

         result.add(element);
         offset++;
      }
      return result;
   }

   public void trim(long from, long to) {
      // from and to are + but from is bigger
      // example: from 2 > to 1 -> empty
      // from and to are - and to is smaller
      // example: from -1 > to -2 -> empty
      if ((from > 0 && to > 0 && from > to) || (from < 0 && to < 0 && from > to)) {
         values.clear();
         return;
      }

      // index request
      if (from == to) {
         V element = index(from);
         if (element != null) {
            values.clear();
            values.add(element);
         }
         return;
      }

      long startRemoveCount = from < 0 ? values.size() + from : from;
      long keepCount = (to < 0 ? values.size() + to : to) - startRemoveCount;

      Iterator<V> ite = values.iterator();
      while(ite.hasNext() && startRemoveCount > 0) {
         ite.next();
         ite.remove();
         startRemoveCount--;
      }

      // keep elements
      while(ite.hasNext() && keepCount >= 0) {
         ite.next();
         keepCount--;
      }

      // remove remaining elements
      while(ite.hasNext()) {
         ite.next();
         ite.remove();
      }
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
      Deque<V> newValues = new ArrayDeque<>(values.size() +1);

      Iterator<V> iterator = values.iterator();
      boolean found = false;
      while (iterator.hasNext()) {
         V next = iterator.next();
         if (found) {
            newValues.offerLast(next);
         } else {
            if (Objects.deepEquals(pivot, next))  {
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

   public long remove(long count, V element) {
      Iterator<V> ite;

      if (count < 0) {
         ite = values.descendingIterator();
      } else {
         ite = values.iterator();
      }

      long maxRemovalsCount = count == 0 ?  values.size(): Math.abs(count);
      long removedElements = 0;
      while(ite.hasNext()) {
         V next = ite.next();
         if (Objects.deepEquals(next, element)) {
            ite.remove();
            removedElements++;
            if (removedElements == maxRemovalsCount) {
               break;
            }
         }
      }

      return removedElements;
   }

   public V rotate(boolean rotateRight) {
      V element;
      if (rotateRight) {
         // from head to tail
         element = values.pollFirst();
         values.offerLast(element);
      } else {
         // from tail to head
         element = values.pollLast();
         values.offerFirst(element);
      }
      return element;
   }

   public class ListBucketResult {
      private final Collection<V> result;
      private final ListBucket<V> bucketValue;
      public ListBucketResult(Collection<V> result, ListBucket<V> bucketValue) {
         this.result = result;
         this.bucketValue = bucketValue;
      }

      public ListBucket<V> bucketValue() {
         return bucketValue;
      }

      public Collection<V> opResult() {
         return result;
      }
   }

   public ListBucketResult poll(boolean first, long count) {
      List<V> polledValues = new ArrayList<>();
      if (count >= values.size()) {
         if (first) {
            polledValues.addAll(values);
         } else {
            Iterator<V> ite = values.descendingIterator();
            while(ite.hasNext()) {
               polledValues.add(ite.next());
            }
         }
         return new ListBucketResult(polledValues, new ListBucket<>());
      }

      for (int i = 0 ; i < count; i++) {
         if (first) {
            polledValues.add(values.pollFirst());
         } else {
            polledValues.add(values.pollLast());
         }
      }
      return new ListBucketResult(polledValues, new ListBucket<>(values));
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

}
