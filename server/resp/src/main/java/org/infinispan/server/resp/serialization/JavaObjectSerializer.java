package org.infinispan.server.resp.serialization;

/**
 * Base class for serializing custom Java objects into the final RESP3 format.
 *
 * <p>
 * Extend this class to add support to a Java object type. This mechanism is useful when handling methods that return
 * an object instead of the primitive values. Also, nested data structures with heterogeneous elements must provide
 * an instance of {@link JavaObjectSerializer} to correctly the elements.
 * </p>
 *
 * An example of how to utilize the serializer when handling a heterogeneous map. The map contains string and integer
 * values.
 *
 * <pre>
 * {@code
 * class MyCustomSerializer extends JavaObjectSerializer<Map<String, Object>> {
 *
 *    @Override
 *    public void accept(Date date, ByteBufPool alloc) {
 *       // Write map prefix, then the keys and values.
 *       // Utilize the base pieces from the Resp3Response class to write the primitive values.
 *    }
 *
 *    public static void main(String[] args) {
 *       ByteBufPool alloc = ...;
 *       Map<String, Object> map = Map.of("k1", "v1", "k2", 42);
 *       Resp3Response.write(map, alloc, new MyCustomSerializer());
 *       Resp3Response.map(map, alloc, new MyCustomSerializer());
 *    }
 * }
 * }
 * </pre>
 *
 * <p>
 * The caller can also provide a lambda that receives the response and the {@link org.infinispan.server.resp.ByteBufPool}
 * as arguments for serialization. If a serializer is stateless, it can be a singleton and shared for all invocations.
 * </p>
 *
 * @param <T> The type of object the instance is handling. This type filter the objects during runtime, accepting types
 *           of T and subclasses.
 */
@FunctionalInterface
public interface JavaObjectSerializer<T> extends ResponseSerializer<T, ResponseWriter> {

   @Override
   default boolean test(Object object) {
      return true;
   }
}
