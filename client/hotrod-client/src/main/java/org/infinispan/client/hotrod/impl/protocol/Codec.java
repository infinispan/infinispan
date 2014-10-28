package org.infinispan.client.hotrod.impl.protocol;

import org.infinispan.client.hotrod.Flag;
import org.infinispan.client.hotrod.event.ClientEvent;
import org.infinispan.client.hotrod.impl.transport.Transport;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.util.Either;

/**
 * A Hot Rod protocol encoder/decoder.
 *
 * @author Galder Zamarreño
 * @since 5.1
 */
public interface Codec {

   /**
    * Writes a request header with the given parameters to the transport and
    * returns an updated header parameters.
    */
   HeaderParams writeHeader(Transport transport, HeaderParams params);

   /**
    * Reads a response header from the transport and returns the status
    * of the response.
    */
   short readHeader(Transport transport, HeaderParams params);

   ClientEvent readEvent(Transport transport, byte[] expectedListenerId, Marshaller marshaller);

   Either<Short, ClientEvent> readHeaderOrEvent(Transport transport, HeaderParams params, byte[] expectedListenerId, Marshaller marshaller);

   byte[] returnPossiblePrevValue(Transport transport, short status, Flag[] flags);

   /**
    * Logger for Hot Rod client codec
    */
   Log getLog();

}
