package org.infinispan.server.resp.serialization.bytebuf;

import java.util.Collection;
import java.util.Set;

import org.infinispan.server.resp.ByteBufPool;
import org.infinispan.server.resp.serialization.NestedResponseSerializer;
import org.infinispan.server.resp.serialization.RespConstants;
import org.infinispan.server.resp.serialization.SerializationHint;

/**
 * Serialize collection types into the RESP3 equivalent.
 *
 * @since 15.0
 * @author José Bolina
 */
final class ByteBufCollectionSerializer {

   /**
    * Abstract the serialization of collections.
    *
    * @param objects: The values to serialize.
    * @param alloc: The buffer allocator.
    * @param symbol: The prefix symbol that represents the collection type.
    */
   private static void serialize(Collection<?> objects, ByteBufPool alloc, byte symbol, SerializationHint.SimpleHint hint) {
      ByteBufResponseWriter writer  = new ByteBufResponseWriter(alloc);
      // First, writes the prefix.
      // RESP: <symbol><number-of-elements>\r\n
      writer.writeNumericPrefix(symbol, objects.size());

      // Write each element individually.
      // The values are heterogeneous, they can be primitives or another aggregate type.
      for (Object object : objects) {
         hint.serialize(object, writer);
      }
   }

   /**
    * Represent an ordered aggregate of elements.
    *
    * <p>
    * The prefix is an asterisk ({@link RespConstants#ARRAY}), followed by the base-10 representation of the number of elements
    * in the list and the terminator. The collection is heterogeneous and contains varying types of values. Therefore, each
    * collection element is serialized individually, following the correct representation.
    * </p>
    */
   static final class ArraySerializer implements NestedResponseSerializer<Collection<?>, ByteBufPool, SerializationHint.SimpleHint> {
      static final ArraySerializer INSTANCE = new ArraySerializer();

      @Override
      public void accept(Collection<?> objects, ByteBufPool alloc, SerializationHint.SimpleHint hint) {
         // RESP: *<number-of-elements>\r\n<element-1>...<element-n>
         serialize(objects, alloc, RespConstants.ARRAY, hint);
      }

      @Override
      public boolean test(Object object) {
         // Handle any collection which is not a set.
         return object instanceof Collection<?> && !(object instanceof Set<?>);
      }
   }

   /**
    * Represent an unordered aggregate of unique elements.
    *
    * <p>
    * Sets behave like {@link ArraySerializer}, but the elements do not have an order, and the collection contains strictly
    * unique elements. The prefix for a set is the tilde symbol ({@link RespConstants#SET}).
    * </p>
    */
   static final class SetSerializer implements NestedResponseSerializer<Set<?>, ByteBufPool, SerializationHint.SimpleHint> {
      static final SetSerializer INSTANCE = new SetSerializer();

      @Override
      public void accept(Set<?> objects, ByteBufPool alloc, SerializationHint.SimpleHint hint) {
         // RESP: ~<number-of-elements>\r\n<element-1>...<element-n>
         serialize(objects, alloc, RespConstants.SET, hint);
      }

      @Override
      public boolean test(Object object) {
         // Accept any instance of sets.
         return object instanceof Set<?>;
      }
   }
}
