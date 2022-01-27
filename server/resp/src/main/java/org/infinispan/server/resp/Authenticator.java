package org.infinispan.server.resp;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.CompletionStage;

import javax.security.auth.Subject;


/**
 * Authentication mechanism.
 */
public interface Authenticator extends Closeable {

   /**
    * Performs authentication using the supplied credentials and returns the authenticated {@link Subject}
    */
   CompletionStage<Subject> authenticate(String username, char[] password);

   /**
    * Invoked by the {@link RespServer} on startup. Can perform additional configuration
    * @param respServer
    */
   default void init(RespServer respServer) {}

   @Override
   default void close() throws IOException {
      // No-op
   }
}
