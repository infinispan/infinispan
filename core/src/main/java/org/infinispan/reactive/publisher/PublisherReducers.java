package org.infinispan.reactive.publisher;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.commons.marshall.Ids;
import org.infinispan.reactive.RxJavaInterop;
import org.reactivestreams.Publisher;

import io.reactivex.Flowable;

/**
 * Static factory method class to provide various reducers for use with distributed Publisher.
 * @author wburns
 * @since 10.0
 */
public class PublisherReducers {
   private PublisherReducers() { }

   public static Function<Publisher<?>, CompletionStage<Long>> sumReducer() {
      return SumReducer.INSTANCE;
   }

   public static Function<Publisher<Long>, CompletionStage<Long>> sumFinalizer() {
      return SumFinalizer.INSTANCE;
   }

   private static class SumReducer implements Function<Publisher<?>, CompletionStage<Long>> {
      private static final SumReducer INSTANCE = new SumReducer();

      @Override
      public CompletionStage<Long> apply(Publisher<?> longPublisher) {
         return Flowable.fromPublisher(longPublisher)
               .count()
               .to(RxJavaInterop.singleToCompletionStage());
      }
   }

   private static class SumFinalizer implements Function<Publisher<Long>, CompletionStage<Long>> {
      private static final SumFinalizer INSTANCE = new SumFinalizer();

      @Override
      public CompletionStage<Long> apply(Publisher<Long> longPublisher) {
         return Flowable.fromPublisher(longPublisher)
               .reduce((long) 0, Long::sum)
               .to(RxJavaInterop.singleToCompletionStage());
      }
   }

   public static final class PublisherReducersExternalizer implements AdvancedExternalizer<Object> {
      enum ExternalizerId {
         SUM_REDUCER(SumReducer.class),
         SUM_FINALIZER(SumFinalizer.class),
         ;

         private final Class<?> marshalledClass;

         ExternalizerId(Class<?> marshalledClass) {
            this.marshalledClass = marshalledClass;
         }
      }

      private static final ExternalizerId[] VALUES = ExternalizerId.values();

      private final Map<Class<?>, ExternalizerId> objects = new HashMap<>();

      public PublisherReducersExternalizer() {
         for (ExternalizerId id : ExternalizerId.values()) {
            objects.put(id.marshalledClass, id);
         }
      }

      @Override
      public Set<Class<?>> getTypeClasses() {
         return objects.keySet();
      }

      @Override
      public Integer getId() {
         return Ids.PUBLISHER_REDUCERS;
      }

      @Override
      public void writeObject(ObjectOutput output, Object object) throws IOException {
         ExternalizerId id = objects.get(object.getClass());
         if (id == null) {
            throw new IllegalArgumentException("Unsupported class " + object.getClass() + " was provided!");
         }
         output.writeByte(id.ordinal());
      }

      @Override
      public Object readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         int number = input.readUnsignedByte();
         ExternalizerId[] ids = VALUES;
         if (number < 0 || number >= ids.length) {
            throw new IllegalArgumentException("Found invalid number " + number);
         }
         ExternalizerId id = ids[number];
         switch (id) {
            case SUM_REDUCER:
               return SumReducer.INSTANCE;
            case SUM_FINALIZER:
               return SumFinalizer.INSTANCE;
            default:
               throw new IllegalArgumentException("ExternalizerId not supported: " + id);
         }
      }
   }
}
