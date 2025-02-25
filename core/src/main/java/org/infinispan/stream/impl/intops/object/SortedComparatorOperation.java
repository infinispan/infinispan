package org.infinispan.stream.impl.intops.object;

import java.util.Comparator;
import java.util.stream.Stream;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.marshall.protostream.impl.MarshallableObject;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.stream.impl.intops.IntermediateOperation;

import io.reactivex.rxjava3.core.Flowable;

/**
 * Performs sorted operation with a comparator on a regular {@link Stream}
 */
@ProtoTypeId(ProtoStreamTypeIds.STREAM_INTOP_SORTED_COMPARATOR_OPERATION)
public class SortedComparatorOperation<S> implements IntermediateOperation<S, Stream<S>, S, Stream<S>> {
   private final Comparator<? super S> comparator;

   public SortedComparatorOperation(Comparator<? super S> comparator) {
      this.comparator = comparator;
   }

   @ProtoFactory
   SortedComparatorOperation(MarshallableObject<Comparator<? super S>> comparator) {
      this.comparator = MarshallableObject.unwrap(comparator);
   }

   @ProtoField(1)
   MarshallableObject<Comparator<? super S>> getComparator() {
      return MarshallableObject.create(comparator);
   }

   @Override
   public Stream<S> perform(Stream<S> stream) {
      return stream.sorted(comparator);
   }

   @Override
   public Flowable<S> mapFlowable(Flowable<S> input) {
      return input.sorted(comparator);
   }
}
