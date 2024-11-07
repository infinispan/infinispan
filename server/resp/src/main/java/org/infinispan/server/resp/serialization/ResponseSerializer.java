package org.infinispan.server.resp.serialization;

import java.util.function.BiConsumer;
import java.util.function.Predicate;

/**
 * Base interface for RESP3 serializers.
 *
 * <p>
 * The interface combines the {@link BiConsumer} and the {@link Predicate}. The former is responsible to serialize the
 * object in the RESP3 format with a provided buffer allocator. The latter is responsible to check whether the serializer
 * is capable of handling the object.
 * </p>
 *
 * @param <T> The type of object to serialize.
 * @param <O> The output object
 * @author José Bolina
 * @since 15.0
 */
public interface ResponseSerializer<T, O> extends BiConsumer<T, O>, Predicate<Object> { }
