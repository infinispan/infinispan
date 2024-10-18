package org.infinispan.server.resp.serialization;

import java.util.ArrayList;
import java.util.List;

import org.infinispan.server.resp.ByteBufPool;

/**
 * Registry holding all RESP3 serializers.
 *
 * <p>
 * The registry keeps track of all RESP3 serializers in a global collection. Every serializer instance is responsible for
 * registering itself for use during runtime. The registry finds a match in the available candidates for every request to
 * serialize an object. The registry is an internal implementation where the only public method registers a custom serializer.
 * </p>
 *
 * @author José Bolina
 */
public final class Resp3SerializerRegistry {

   private Resp3SerializerRegistry() { }

   private static final ResponseSerializer<?>[] serializers;

   static {
      // The serializers follow the registration order.
      // Therefore, we try to keep the most common first and add any new one later.
      List<ResponseSerializer<?>> s = new ArrayList<>(PrimitiveSerializer.SERIALIZERS);
      s.add(CollectionSerializer.ArraySerializer.INSTANCE);
      s.add(CollectionSerializer.SetSerializer.INSTANCE);
      s.add(DoubleSerializer.INSTANCE);
      s.add(ThrowableSerializer.INSTANCE);
      s.add(MapSerializer.INSTANCE);
      s.add(BigNumberSerializer.INSTANCE);
      serializers = s.toArray(ResponseSerializer[]::new);
   }

   /**
    * Serializes the given object checking the complete registry for a match.
    *
    * @param value The object to serialize in RESP3 format.
    * @param alloc Buffer pool for allocation.
    * @throws IllegalStateException in case no serializer is found for the object.
    */
   static void serialize(Object value, ByteBufPool alloc) {
      serialize(value, alloc, serializers);
   }

   static void serialize(Object value, ByteBufPool alloc, ResponseSerializer<?>[] candidates) {
      // The first check must always be for the null serializer.
      // This removes all null checks on every other serializer.
      if (PrimitiveSerializer.NullSerializer.INSTANCE.test(value)) {
         PrimitiveSerializer.NullSerializer.INSTANCE.accept(null, alloc);
         return;
      }

      for (ResponseSerializer<?> serializer : candidates) {
         if (serializer.test(value)) {
            serialize(serializer, value, alloc);
            return;
         }
      }

      throw new IllegalStateException("Serializer unknown: " + value.getClass());
   }

   /**
    * Serializes the given object only with the provided serializer.
    *
    * @param value The object to serialize in RESP3 format.
    * @param alloc Buffer pool for allocation.
    * @param candidate The candidate serializer to handle the object.
    * @throws IllegalStateException: when the candidate does not accept the provided value.
    */
   static void serialize(Object value, ByteBufPool alloc, ResponseSerializer<?> candidate) {
      // The first check must always be for the null serializer.
      // This removes all null checks on every other serializer.
      if (PrimitiveSerializer.NullSerializer.INSTANCE.test(value)) {
         PrimitiveSerializer.NullSerializer.INSTANCE.accept(null, alloc);
         return;
      }

      if (!candidate.test(value))
         throw new IllegalStateException("Serializer not handling: " + value.getClass());

      serialize(candidate, value, alloc);
   }

   static <H extends SerializationHint> void serialize(Object value, ByteBufPool alloc,
                                                       NestedResponseSerializer<?, H> candidate, H hint) {

      // The first check must always be for the null serializer.
      // This removes all null checks on every other serializer.
      if (PrimitiveSerializer.NullSerializer.INSTANCE.test(value)) {
         PrimitiveSerializer.NullSerializer.INSTANCE.accept(null, alloc);
         return;
      }

      if (!candidate.test(value))
         throw new IllegalStateException("Serializer not handling: " + value.getClass());

      @SuppressWarnings("unchecked")
      NestedResponseSerializer<Object, H> nrs = (NestedResponseSerializer<Object, H>) candidate;
      nrs.accept(value, alloc, hint);
   }

   private static void serialize(ResponseSerializer<?> serializer, Object value, ByteBufPool alloc) {
      @SuppressWarnings("unchecked")
      ResponseSerializer<Object> s = (ResponseSerializer<Object>) serializer;
      s.accept(value, alloc);
   }
}
