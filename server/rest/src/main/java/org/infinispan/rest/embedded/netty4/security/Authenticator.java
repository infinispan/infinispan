package org.infinispan.rest.embedded.netty4.security;

import java.io.IOException;

import javax.ws.rs.core.SecurityContext;

import org.jboss.resteasy.spi.HttpRequest;
import org.jboss.resteasy.spi.HttpResponse;

import io.netty.channel.ChannelHandlerContext;

/**
 * Authenticator
 *
 * @author Tristan Tarrant
 * @since 9.0
 */
public interface Authenticator {
   SecurityContext authenticate(ChannelHandlerContext ctx, HttpRequest request, HttpResponse response) throws IOException;
}
