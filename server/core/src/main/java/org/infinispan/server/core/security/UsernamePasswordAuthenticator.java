package org.infinispan.server.core.security;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.CompletionStage;

import javax.security.auth.Subject;

import org.infinispan.server.core.ProtocolServer;


/**
 * Authentication mechanism.
 */
public interface UsernamePasswordAuthenticator extends Closeable {

   /**
    * Performs authentication using the supplied credentials and returns the authenticated {@link Subject}
    */
   CompletionStage<Subject> authenticate(String username, char[] password);

   /**
    * Invoked by the {@link ProtocolServer} on startup. Can perform additional configuration
    * @param server
    */
   default void init(ProtocolServer<?> server) {}

   @Override
   default void close() throws IOException {
      // No-op
   }
}
