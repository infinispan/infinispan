package org.infinispan.server.resp.serialization.bytebuf;

import java.util.ArrayList;
import java.util.List;

import org.infinispan.server.resp.ByteBufPool;
import org.infinispan.server.resp.serialization.NestedResponseSerializer;
import org.infinispan.server.resp.serialization.ResponseSerializer;
import org.infinispan.server.resp.serialization.SerializationHint;

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
final class ByteBufSerializerRegistry {

   private ByteBufSerializerRegistry() { }

   private static final ResponseSerializer<?, ByteBufPool>[] serializers;

   static {
      // The serializers follow the registration order.
      // Therefore, we try to keep the most common first and add any new one later.
      List<ResponseSerializer<?, ByteBufPool>> s = new ArrayList<>(ByteBufPrimitiveSerializer.SERIALIZERS);
      s.add(ByteBufCollectionSerializer.ArraySerializer.INSTANCE);
      s.add(ByteBufCollectionSerializer.SetSerializer.INSTANCE);
      s.add(ByteBufDoubleSerializer.INSTANCE);
      s.add(ByteBufThrowableSerializer.INSTANCE);
      s.add(ByteBufMapSerializer.INSTANCE);
      s.add(ByteBufBigNumberSerializer.INSTANCE);
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

   static void serialize(Object value, ByteBufPool alloc, ResponseSerializer<?, ByteBufPool>[] candidates) {
      // The first check must always be for the null serializer.
      // This removes all null checks on every other serializer.
      if (ByteBufPrimitiveSerializer.NullSerializer.INSTANCE.test(value)) {
         ByteBufPrimitiveSerializer.NullSerializer.INSTANCE.accept(null, alloc);
         return;
      }

      for (ResponseSerializer<?, ByteBufPool> serializer : candidates) {
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
   static void serialize(Object value, ByteBufPool alloc, ResponseSerializer<?, ByteBufPool> candidate) {
      // The first check must always be for the null serializer.
      // This removes all null checks on every other serializer.
      if (ByteBufPrimitiveSerializer.NullSerializer.INSTANCE.test(value)) {
         ByteBufPrimitiveSerializer.NullSerializer.INSTANCE.accept(null, alloc);
         return;
      }

      if (!candidate.test(value))
         throw new IllegalStateException("Serializer not handling: " + value.getClass());

      serialize(candidate, value, alloc);
   }

   static <H extends SerializationHint> void serialize(Object value, ByteBufPool alloc,
                                                       NestedResponseSerializer<?, ByteBufPool, H> candidate, H hint) {

      // The first check must always be for the null serializer.
      // This removes all null checks on every other serializer.
      if (ByteBufPrimitiveSerializer.NullSerializer.INSTANCE.test(value)) {
         ByteBufPrimitiveSerializer.NullSerializer.INSTANCE.accept(null, alloc);
         return;
      }

      if (!candidate.test(value))
         throw new IllegalStateException("Serializer not handling: " + value.getClass());

      @SuppressWarnings("unchecked")
      NestedResponseSerializer<Object, ByteBufPool, H> nrs = (NestedResponseSerializer<Object, ByteBufPool, H>) candidate;
      nrs.accept(value, alloc, hint);
   }

   private static void serialize(ResponseSerializer<?, ByteBufPool> serializer, Object value, ByteBufPool alloc) {
      @SuppressWarnings("unchecked")
      ResponseSerializer<Object, ByteBufPool> s = (ResponseSerializer<Object, ByteBufPool>) serializer;
      s.accept(value, alloc);
   }
}
