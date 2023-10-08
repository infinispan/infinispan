package org.infinispan.reactive.publisher.impl.commands.reduction;

import java.util.Set;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.commons.util.IntSet;
import org.infinispan.marshall.protostream.impl.MarshallableObject;
import org.infinispan.marshall.protostream.impl.WrappedMessages;
import org.infinispan.protostream.WrappedMessage;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * A PublisherResult that was performed due to segments only
 * @author wburns
 * @since 10.0
 */
@ProtoTypeId(ProtoStreamTypeIds.SEGMENT_PUBLISHER_RESULT)
public class SegmentPublisherResult<R> implements PublisherResult<R> {
   private final IntSet suspectedSegments;
   private final R result;

   public SegmentPublisherResult(IntSet suspectedSegments, R result) {
      this.suspectedSegments = suspectedSegments;
      this.result = result;
   }

   @ProtoFactory
   SegmentPublisherResult(WrappedMessage wrappedSuspectedSegments, MarshallableObject<R> wrappedObject) {
      this.suspectedSegments = WrappedMessages.unwrap(wrappedSuspectedSegments);
      this.result = MarshallableObject.unwrap(wrappedObject);
   }

   @ProtoField(1)
   WrappedMessage getWrappedSuspectedSegments() {
      return WrappedMessages.orElseNull(suspectedSegments);
   }

   @ProtoField(2)
   MarshallableObject<R> getWrappedObject() {
      return MarshallableObject.create(result);
   }

   @Override
   public IntSet getSuspectedSegments() {
      return suspectedSegments;
   }

   @Override
   public Set<?> getSuspectedKeys() {
      return null;
   }

   @Override
   public R getResult() {
      return result;
   }

   @Override
   public String toString() {
      return "SegmentPublisherResult{" +
            "result=" + result +
            ", suspectedSegments=" + suspectedSegments +
            '}';
   }
}
