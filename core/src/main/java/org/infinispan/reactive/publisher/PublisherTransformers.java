package org.infinispan.reactive.publisher;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.commons.marshall.Ids;
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

   private static final class IdentityTransformer<I> implements Function<Publisher<I>, Publisher<I>> {
      private static final IdentityTransformer INSTANCE = new IdentityTransformer();

      @Override
      public Publisher<I> apply(Publisher<I> publisher) {
         return publisher;
      }
   }

   public static final class PublisherTransformersExternalizer implements AdvancedExternalizer<Object> {
      enum ExternalizerId {
         IDENTITY_TRANSFORMER(IdentityTransformer.class),
         ;

         private final Class<?> marshalledClass;

         ExternalizerId(Class<?> marshalledClass) {
            this.marshalledClass = marshalledClass;
         }
      }

      private static final ExternalizerId[] VALUES = ExternalizerId.values();

      private final Map<Class<?>, ExternalizerId> objects = new HashMap<>();

      public PublisherTransformersExternalizer() {
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
         return Ids.PUBLISHER_TRANSFORMERS;
      }

      @Override
      public void writeObject(ObjectOutput output, Object object) throws IOException {
         ExternalizerId id = objects.get(object.getClass());
         if (id == null) {
            throw new IllegalArgumentException("Unsupported class " + object.getClass() + " was provided!");
         }
         output.writeByte(id.ordinal());
         // add switch case here when we have types that require state
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
            case IDENTITY_TRANSFORMER:
               return IdentityTransformer.INSTANCE;
            default:
               throw new IllegalArgumentException("ExternalizerId not supported: " + id);
         }
      }
   }
}
