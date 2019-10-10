package org.infinispan.stream.impl;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

import org.infinispan.commands.functional.functions.InjectableComponent;
import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.marshall.core.Ids;
import org.infinispan.stream.impl.intops.IntermediateOperation;
import org.reactivestreams.Publisher;

import io.reactivex.Flowable;

/**
 * Reducer implementation for Distributed Publisher that converts between CacheStream operations to an
 * appropriate Reducer
 * @param <R>
 */
public final class CacheStreamIntermediateReducer<R> implements Function<Publisher<Object>, CompletionStage<R>>, InjectableComponent {
   private final Queue<IntermediateOperation> intOps;
   private final Function<? super Publisher<Object>, ? extends CompletionStage<R>> transformer;

   CacheStreamIntermediateReducer(Queue<IntermediateOperation> intOps, Function<? super Publisher<Object>, ? extends CompletionStage<R>> transformer) {
      this.intOps = intOps;
      this.transformer = transformer;
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

   public static final class ReducerExternalizer implements AdvancedExternalizer<CacheStreamIntermediateReducer> {
      @Override
      public void writeObject(ObjectOutput output, CacheStreamIntermediateReducer object) throws IOException {
         output.writeObject(object.intOps);
         output.writeObject(object.transformer);
      }

      @Override
      public CacheStreamIntermediateReducer readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         return new CacheStreamIntermediateReducer((Queue) input.readObject(), (Function) input.readObject());
      }

      @Override
      public Set<Class<? extends CacheStreamIntermediateReducer>> getTypeClasses() {
         return Collections.singleton(CacheStreamIntermediateReducer.class);
      }

      @Override
      public Integer getId() {
         return Ids.CACHE_STREAM_INTERMEDIATE_REDUCER;
      }
   }
}
