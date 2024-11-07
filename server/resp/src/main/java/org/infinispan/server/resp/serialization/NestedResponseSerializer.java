package org.infinispan.server.resp.serialization;

import org.infinispan.util.function.TriConsumer;

/**
 * Serializes a nested Java object into the correct RESP3 representation.
 *
 * <p>
 * A nested object is one composed of other RESP3 elements. For example, an array or a map are nested objects. There is
 * no limitation regarding the number of nested elements that can exist. Therefore, to aid during serialization, a caller
 * can provide hints about the types of inner elements.
 * </p>
 *
 * @param <T> The generic type of the Java object the serializer handles.
 * @param <H> The generic type of hints the serializer accepts.
 * @author José Bolina
 */
public interface NestedResponseSerializer<T, O, H extends SerializationHint>
      extends ResponseSerializer<T, O>, TriConsumer<T, O, H> {

   @Override
   default void accept(T t, O out) {
      throw new IllegalStateException("Nested response without serialization hints");
   }
}
