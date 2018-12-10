package org.infinispan.reactive.publisher.impl;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Set;

import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.commons.marshall.Ids;
import org.infinispan.commons.util.IntSet;

/**
 * A PublisherResult that was performed due to segments only
 * @author wburns
 * @since 10.0
 */
public class SegmentPublisherResult<R> implements PublisherResult<R> {
   private final IntSet suspectedSegments;
   private final R result;

   public SegmentPublisherResult(IntSet suspectedSegments, R result) {
      this.suspectedSegments = suspectedSegments;
      this.result = result;
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

   public static class Externalizer implements AdvancedExternalizer<SegmentPublisherResult> {

      @Override
      public Set<Class<? extends SegmentPublisherResult>> getTypeClasses() {
         return Collections.singleton(SegmentPublisherResult.class);
      }

      @Override
      public Integer getId() {
         return Ids.SIMPLE_PUBLISHER_RESULT;
      }

      @Override
      public void writeObject(ObjectOutput output, SegmentPublisherResult object) throws IOException {
         output.writeObject(object.suspectedSegments);
         output.writeObject(object.result);
      }

      @Override
      public SegmentPublisherResult readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         return new SegmentPublisherResult<>((IntSet) input.readObject(), input.readObject());
      }
   }
}
