package org.infinispan.rest.embedded.netty4;

import java.io.IOException;

import javax.ws.rs.core.SecurityContext;

import org.infinispan.rest.embedded.netty4.security.Authenticator;
import org.jboss.resteasy.core.SynchronousDispatcher;
import org.jboss.resteasy.core.ThreadLocalResteasyProviderFactory;
import org.jboss.resteasy.spi.HttpRequest;
import org.jboss.resteasy.spi.HttpResponse;
import org.jboss.resteasy.spi.ResteasyProviderFactory;

import io.netty.channel.ChannelHandlerContext;

/**
 * Helper/delegate class to unify Servlet and Filter dispatcher implementations
 *
 * @author <a href="mailto:bill@burkecentral.com">Bill Burke</a>
 * @author Norman Maurer
 * @version $Revision: 1 $
 * Temporary fork from RestEasy 3.1.0
 */
public class RequestDispatcher {
   protected final SynchronousDispatcher dispatcher;
   protected final ResteasyProviderFactory providerFactory;
   private final Authenticator authenticator;

   public RequestDispatcher(SynchronousDispatcher dispatcher, ResteasyProviderFactory providerFactory, Authenticator authenticator) {
      this.dispatcher = dispatcher;
      this.providerFactory = providerFactory;
      this.authenticator = authenticator;
   }

   public SynchronousDispatcher getDispatcher() {
      return dispatcher;
   }

   public Authenticator getAuthenticator() {
      return authenticator;
   }

   public ResteasyProviderFactory getProviderFactory() {
      return providerFactory;
   }

   public void service(ChannelHandlerContext ctx, HttpRequest request, HttpResponse response, boolean handleNotFound) throws IOException {

      try {
         ResteasyProviderFactory defaultInstance = ResteasyProviderFactory.getInstance();
         if (defaultInstance instanceof ThreadLocalResteasyProviderFactory) {
            ThreadLocalResteasyProviderFactory.push(providerFactory);
         }

         SecurityContext securityContext;
         if (authenticator != null) {
            securityContext = authenticator.authenticate(ctx, request, response);
            if (securityContext == null)
               return; // Authentication failed
         } else {
            securityContext = NettySecurityContext.ANONYMOUS;
         }

         try {
            ResteasyProviderFactory.pushContext(SecurityContext.class, securityContext);
            ResteasyProviderFactory.pushContext(ChannelHandlerContext.class, ctx);
            if (handleNotFound) {
               dispatcher.invoke(request, response);
            } else {
               dispatcher.invokePropagateNotFound(request, response);
            }
         } finally {
            ResteasyProviderFactory.clearContextData();
         }
      } finally {
         ResteasyProviderFactory defaultInstance = ResteasyProviderFactory.getInstance();
         if (defaultInstance instanceof ThreadLocalResteasyProviderFactory) {
            ThreadLocalResteasyProviderFactory.pop();
         }

      }
   }

}
