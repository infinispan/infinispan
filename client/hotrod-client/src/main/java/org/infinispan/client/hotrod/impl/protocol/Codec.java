package org.infinispan.client.hotrod.impl.protocol;

import java.lang.annotation.Annotation;
import java.net.SocketAddress;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.infinispan.client.hotrod.annotation.ClientListener;
import org.infinispan.client.hotrod.counter.impl.HotRodCounterEvent;
import org.infinispan.client.hotrod.event.ClientEvent;
import org.infinispan.client.hotrod.impl.transport.netty.ChannelFactory;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.util.Either;

import io.netty.buffer.ByteBuf;

/**
 * A Hot Rod protocol encoder/decoder.
 *
 * @author Galder Zamarreño
 * @since 5.1
 */
public interface Codec {

   int estimateHeaderSize(HeaderParams headerParams);

   /**
    * Writes a request header with the given parameters to the transport and
    * returns an updated header parameters.
    */
   HeaderParams writeHeader(ByteBuf buf, HeaderParams params);

   /**
    * Writes client listener parameters
    */
   void writeClientListenerParams(ByteBuf buf, ClientListener clientListener,
                                  byte[][] filterFactoryParams, byte[][] converterFactoryParams);

   /**
    * Write lifespan/maxidle parameters.
    */
   void writeExpirationParams(ByteBuf buf, long lifespan, TimeUnit lifespanTimeUnit, long maxIdle, TimeUnit maxIdleTimeUnit);

   int estimateExpirationSize(long lifespan, TimeUnit lifespanTimeUnit, long maxIdle, TimeUnit maxIdleTimeUnit);

   long readMessageId(ByteBuf buf);

   /**
    * Reads a response header from the transport and returns the status
    * of the response.
    */
   short readHeader(ByteBuf buf, HeaderParams params, ChannelFactory channelFactory, SocketAddress serverAddress);

   ClientEvent readEvent(ByteBuf buf, byte[] expectedListenerId, Marshaller marshaller, List<String> whitelist, SocketAddress serverAddress);

   Either<Short, ClientEvent> readHeaderOrEvent(ByteBuf buf, HeaderParams params, byte[] expectedListenerId, Marshaller marshaller, List<String> whitelist, ChannelFactory channelFactory, SocketAddress serverAddress);

   Object returnPossiblePrevValue(ByteBuf buf, short status, int flags, List<String> whitelist, Marshaller marshaller);

   /**
    * Logger for Hot Rod client codec
    */
   Log getLog();

   /**
    * Read and unmarshall byte array.
    */
   <T> T readUnmarshallByteArray(ByteBuf buf, short status, List<String> whitelist, Marshaller marshaller);

   void writeClientListenerInterests(ByteBuf buf, Set<Class<? extends Annotation>> classes);

   /**
    * Reads a {@link HotRodCounterEvent} with the {@code listener-id}.
    */
   HotRodCounterEvent readCounterEvent(ByteBuf buf, byte[] listenerId);
}
