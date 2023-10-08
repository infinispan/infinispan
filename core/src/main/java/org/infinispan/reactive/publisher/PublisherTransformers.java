package org.infinispan.reactive.publisher;

import java.util.function.Function;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.reactivestreams.Publisher;

/**
 * Static factory method class to provide various transformers for use with distributed Publisher. Note
 * that these functions are all serializable by Infinispan assuming that any passed arguments are as well.
 * @author wburns
 * @since 11.0
 */
public class PublisherTransformers {
   private PublisherTransformers() { }

   public static <I> Function<Publisher<I>, Publisher<I>> identity() {
      return IdentityTransformer.INSTANCE;
   }

   @ProtoTypeId(ProtoStreamTypeIds.PUBLISHER_TRANSFORMERS_IDENTITY_TRANSFORMER)
   public static final class IdentityTransformer<I> implements Function<Publisher<I>, Publisher<I>> {
      private static final IdentityTransformer INSTANCE = new IdentityTransformer();

      @ProtoFactory
      static IdentityTransformer getInstance() {
         return INSTANCE;
      }

      @Override
      public Publisher<I> apply(Publisher<I> publisher) {
         return publisher;
      }
   }
}
