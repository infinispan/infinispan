package org.infinispan.query;

/**
 * Converts objects (cache keys only) to and from their Java types to String representations so that Infinispan can index them.
 * You need to convert custom types only.
 * Infinispan transforms boxed primitives, java.lang.String, java.util.UUID, and byte arrays internally.
 * <p>
 * Implementations must be thread-safe! It is recommended they are also stateless.
 *
 * @author Manik Surtani
 * @since 4.0
 * @deprecated use {@link org.infinispan.api.query.Transformer} instead
 */
@Deprecated
public interface Transformer extends org.infinispan.api.query.Transformer {
}
