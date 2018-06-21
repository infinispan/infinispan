package org.infinispan.query.remote.client;

import java.io.IOException;

import org.infinispan.protostream.SerializationContext;

/**
 * Support for custom initialization of the server cache manager's {@link SerializationContext}.
 *
 * <p>Implementations of this interface are discovered using the JDK's {@link java.util.ServiceLoader} utility,
 * and should have a file called <pre>org.infinispan.query.remote.client.ProtostreamSerializationContextInitializer</pre> in the
 * <pre>META-INF/services/</pre> folder in their jar, containing the fully qualified class name of the implementation.
 * </p>
 *
 * @since 9.3
 */
public interface ProtostreamSerializationContextInitializer {

   void init(SerializationContext serializationContext) throws IOException;

}
