package org.infinispan.client.hotrod.impl.transport.netty.singleport;

import io.netty.buffer.ByteBufAllocator;
import io.netty.handler.ssl.OpenSsl;
import io.netty.handler.ssl.SslHandler;

/**
 * Creates SSLHandler
 *
 * <p>
 *    The SSLHandler can be created with different ways - with or without ALPN, based on OpenSSL or JDK etc.
 *    This will probably get unified after JDK9+ when we get support for ALPN. However, till then, we need
 *    To use some workarounds...
 * </p>
 *
 * @author Sebastian Łaskawiec
 */
public interface SslHandlerFactory {

   SslHandler getSslHandler(ByteBufAllocator alloc);

   default String getSslHandlerName() {
      return SslHandler.class.getSimpleName();
   }

   default void assertAlpnSupported() {
      if (!OpenSsl.isAlpnSupported()) {
         throw new IllegalStateException("OpenSSL is not present, can not use TLS/ALPN. Version: " + OpenSsl.versionString() + " Cause: " + OpenSsl.unavailabilityCause());
      }
   }

}
