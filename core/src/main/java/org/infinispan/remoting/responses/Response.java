package org.infinispan.remoting.responses;

/**
 * A response to be sent back to a remote caller
 *
 * @author Manik Surtani
 * @since 4.0
 */
public interface Response {

   boolean isSuccessful();

   boolean isValid();
}
