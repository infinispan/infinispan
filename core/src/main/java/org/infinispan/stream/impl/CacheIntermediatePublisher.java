package org.infinispan.stream.impl;

import java.util.ArrayDeque;
import java.util.Collection;

import org.infinispan.commands.functional.functions.InjectableComponent;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.marshall.protostream.impl.MarshallableCollection;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.reactive.publisher.impl.ModifiedValueFunction;
import org.infinispan.stream.impl.intops.IntermediateOperation;
import org.infinispan.stream.impl.intops.MappingOperation;
import org.infinispan.stream.impl.intops.object.MapOperation;
import org.reactivestreams.Publisher;

import io.reactivex.rxjava3.core.Flowable;

/**
 * Function that is used to encapsulate multiple intermediate operations and perform them lazily when the function
 * is applied.
 * @param <R>
 */
@ProtoTypeId(ProtoStreamTypeIds.CACHE_INTERMEDIATE_PUBLISHER)
public final class CacheIntermediatePublisher<R> implements ModifiedValueFunction<Publisher<Object>, Publisher<R>>, InjectableComponent {
   private final Collection<IntermediateOperation<?, ?, ?, ?>> intOps;

   public CacheIntermediatePublisher(Collection<IntermediateOperation<?, ?, ?, ?>> intOps) {
      this.intOps = intOps;
   }

   @ProtoFactory
   CacheIntermediatePublisher(MarshallableCollection<IntermediateOperation<?, ?, ?, ?>> intermediateOperations) {
      this.intOps = MarshallableCollection.unwrap(intermediateOperations, ArrayDeque::new);
   }

   @ProtoField(number = 1)
   MarshallableCollection<IntermediateOperation<?, ?, ?, ?>> getIntermediateOperations() {
      return MarshallableCollection.create(intOps);
   }

   @Override
   public Publisher<R> apply(Publisher<Object> objectPublisher) {
      Flowable<Object> innerPublisher = Flowable.fromPublisher(objectPublisher);
      for (IntermediateOperation<?, ?, ?, ?> intOp : intOps) {
         innerPublisher = intOp.mapFlowable((Flowable) innerPublisher);
      }
      return (Publisher<R>) innerPublisher;
   }

   @Override
   public boolean isModified() {
      for (IntermediateOperation intOp : intOps) {
         if (intOp instanceof MappingOperation) {
            // Encoding functions retain the original value - so we ignore those
            if (intOp instanceof MapOperation && ((MapOperation) intOp).isEncodingFunction()) {
               continue;
            }
            return true;
         }
      }
      return false;
   }

   @Override
   public void inject(ComponentRegistry registry) {
      for (IntermediateOperation intOp : intOps) {
         intOp.handleInjection(registry);
      }
   }
}
