package org.infinispan.stream.impl;

import java.util.Queue;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

import org.infinispan.commands.functional.functions.InjectableComponent;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.marshall.protostream.impl.MarshallableDeque;
import org.infinispan.marshall.protostream.impl.MarshallableObject;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.stream.impl.intops.IntermediateOperation;
import org.reactivestreams.Publisher;

import io.reactivex.rxjava3.core.Flowable;

/**
 * Reducer implementation for Distributed Publisher that converts between CacheStream operations to an
 * appropriate Reducer
 * @param <R>
 */
@ProtoTypeId(ProtoStreamTypeIds.CACHE_STREAM_INTERMEDIATE_REDUCER)
public final class CacheStreamIntermediateReducer<R> implements Function<Publisher<Object>, CompletionStage<R>>, InjectableComponent {
   private final Queue<IntermediateOperation> intOps;
   private final Function<? super Publisher<Object>, ? extends CompletionStage<R>> transformer;

   CacheStreamIntermediateReducer(Queue<IntermediateOperation> intOps, Function<? super Publisher<Object>, ? extends CompletionStage<R>> transformer) {
      this.intOps = intOps;
      this.transformer = transformer;
   }

   @ProtoFactory
   CacheStreamIntermediateReducer(MarshallableDeque<IntermediateOperation> intermediateOperations,
                                  MarshallableObject<Function<? super Publisher<Object>, ? extends CompletionStage<R>>> transformer) {
      this.intOps = MarshallableDeque.unwrap(intermediateOperations);
      this.transformer = MarshallableObject.unwrap(transformer);
   }

   @ProtoField(1)
   MarshallableDeque<IntermediateOperation> getIntermediateOperations() {
      return MarshallableDeque.create(intOps);
   }

   @ProtoField(2)
   MarshallableObject<Function<? super Publisher<Object>, ? extends CompletionStage<R>>> getTransformer() {
      return MarshallableObject.create(transformer);
   }

   @Override
   public CompletionStage<R> apply(Publisher<Object> objectPublisher) {
      Flowable<Object> innerPublisher = Flowable.fromPublisher(objectPublisher);
      for (IntermediateOperation intOp : intOps) {
         innerPublisher = intOp.mapFlowable(innerPublisher);
      }
      return transformer.apply(innerPublisher);
   }

   @Override
   public void inject(ComponentRegistry registry) {
      for (IntermediateOperation intOp : intOps) {
         intOp.handleInjection(registry);
      }
   }
}
