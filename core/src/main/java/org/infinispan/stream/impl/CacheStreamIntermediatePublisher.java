package org.infinispan.stream.impl;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Queue;
import java.util.Set;

import org.infinispan.cache.impl.EncodingFunction;
import org.infinispan.commands.functional.functions.InjectableComponent;
import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.marshall.core.Ids;
import org.infinispan.reactive.publisher.impl.ModifiedValueFunction;
import org.infinispan.stream.impl.intops.IntermediateOperation;
import org.infinispan.stream.impl.intops.MappingOperation;
import org.infinispan.stream.impl.intops.object.MapOperation;
import org.reactivestreams.Publisher;

import io.reactivex.Flowable;

/**
 * Function that is used to bridge DistributedCacheStream and CluserPublisherManager worlds.
 * @param <R>
 */
public final class CacheStreamIntermediatePublisher<R> implements ModifiedValueFunction<Publisher<Object>, Publisher<R>>, InjectableComponent {
   private final Queue<IntermediateOperation> intOps;

   CacheStreamIntermediatePublisher(Queue<IntermediateOperation> intOps) {
      this.intOps = intOps;
   }

   @Override
   public Publisher<R> apply(Publisher<Object> objectPublisher) {
      Flowable<Object> innerPublisher = Flowable.fromPublisher(objectPublisher);
      for (IntermediateOperation intOp : intOps) {
         innerPublisher = intOp.mapFlowable(innerPublisher);
      }
      return (Publisher<R>) innerPublisher;
   }

   @Override
   public boolean isModified() {
      for (IntermediateOperation intOp : intOps) {
         if (intOp instanceof MappingOperation) {
            // Encoding functions retain the original value - so we ignore those
            if (intOp instanceof MapOperation && ((MapOperation) intOp).getFunction() instanceof EncodingFunction) {
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

   public static final class ReducerExternalizer implements AdvancedExternalizer<CacheStreamIntermediatePublisher> {
      @Override
      public void writeObject(ObjectOutput output, CacheStreamIntermediatePublisher object) throws IOException {
         output.writeObject(object.intOps);
      }

      @Override
      public CacheStreamIntermediatePublisher readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         return new CacheStreamIntermediatePublisher((Queue) input.readObject());
      }

      @Override
      public Set<Class<? extends CacheStreamIntermediatePublisher>> getTypeClasses() {
         return Collections.singleton(CacheStreamIntermediatePublisher.class);
      }

      @Override
      public Integer getId() {
         return Ids.CACHE_STREAM_INTERMEDIATE_PUBLISHER;
      }
   }
}
