package org.infinispan.query.remote.client;

import java.io.IOException;

import org.infinispan.protostream.SerializationContext;

/**
 * Support for custom initialization of the server cache manager's {@link SerializationContext}. This is a hook for
 * users to be able to add their own Protobuf definitions and register marshallers at startup.
 *
 * <p>
 * Implementations of this interface are discovered using the JDK's {@link java.util.ServiceLoader} utility, and should
 * have a file called <pre>org.infinispan.query.remote.client.ProtostreamSerializationContextInitializer</pre> in the
 * <pre>META-INF/services/</pre> folder in their jar, containing the fully qualified class name of the implementation.
 *
 * @since 9.3
 */
public interface ProtostreamSerializationContextInitializer {

   void init(SerializationContext serializationContext) throws IOException;
}
