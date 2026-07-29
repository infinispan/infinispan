package org.infinispan.server.core.transport;

import javax.net.ssl.SSLParameters;

import org.infinispan.commons.jdkspecific.SSLParametersHelper;

import io.netty.buffer.ByteBufAllocator;
import io.netty.handler.ssl.SniHandler;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.Mapping;

/**
 * @since 16.3
 */
class NamedGroupsSniHandler extends SniHandler {

   private final String[] namedGroups;

   NamedGroupsSniHandler(Mapping<? super String, ? extends SslContext> mapping, String[] namedGroups) {
      super(mapping);
      this.namedGroups = namedGroups;
   }

   @Override
   protected SslHandler newSslHandler(SslContext context, ByteBufAllocator allocator) {
      SslHandler handler = super.newSslHandler(context, allocator);
      SSLParameters params = handler.engine().getSSLParameters();
      SSLParametersHelper.setNamedGroups(params, namedGroups);
      handler.engine().setSSLParameters(params);
      return handler;
   }
}
