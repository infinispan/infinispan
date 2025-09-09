package org.infinispan.server.memcached.binary;

import static org.infinispan.server.memcached.binary.BinaryConstants.MEMCACHED_SASL_PROTOCOL;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

import javax.security.auth.Subject;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;

import org.infinispan.commons.util.Util;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.server.core.security.sasl.SaslAuthenticator;
import org.infinispan.server.core.security.sasl.SubjectSaslServer;
import org.infinispan.server.core.transport.SaslQopHandler;
import org.infinispan.server.memcached.MemcachedResponse;
import org.infinispan.server.memcached.MemcachedServer;
import org.infinispan.server.memcached.MemcachedStatus;

abstract class BinaryAuthDecoder extends BinaryDecoder {
   private SaslServer saslServer;

   protected BinaryAuthDecoder(MemcachedServer server) {
      super(server, ANONYMOUS);
   }

   protected MemcachedResponse saslListMechs(BinaryHeader header) {
      byte[] mechs = String.join(" ", server.getConfiguration().authentication().sasl().mechanisms()).getBytes(StandardCharsets.US_ASCII);
      response(header, MemcachedStatus.NO_ERROR, mechs);
      return send(header, CompletableFutures.completedNull());
   }

   protected MemcachedResponse saslAuth(BinaryHeader header, byte[] mech, byte[] data) {
      CompletionStage<?> r = server.getBlockingManager()
            .supplyBlocking(() -> {
               try {
                  saslServer = SaslAuthenticator.createSaslServer(server.getConfiguration().authentication().sasl(), ctx.channel(), new String(mech, StandardCharsets.US_ASCII), MEMCACHED_SASL_PROTOCOL);
                  return doSasl(header, data);
               } catch (Throwable t) {
                  return CompletableFuture.failedFuture(new SecurityException(t));
               }
            }, "memcached-sasl-auth")
            .thenCompose(c -> c);
      return send(header, r);
   }

   protected MemcachedResponse saslStep(BinaryHeader header, byte[] mech, byte[] data) {
      CompletionStage<?> r = server.getBlockingManager()
            .supplyBlocking(() -> doSasl(header, data), "memcached-sasl-step")
            .thenCompose(Function.identity());
      return send(header, r);
   }

   private CompletionStage<MemcachedResponse.ResponseWriter> doSasl(BinaryHeader header, byte[] data) {
      try {
         byte[] serverChallenge = saslServer.evaluateResponse(data);
         if (!saslServer.isComplete()) {
            return CompletableFuture.completedFuture(allocator -> response(header, MemcachedStatus.AUTHN_CONTINUE, serverChallenge));
         }

         // Obtaining the subject might be expensive, so do it before sending the final server challenge, otherwise the
         // client might send another operation before this is complete
         Subject subject = (Subject) saslServer.getNegotiatedProperty(SubjectSaslServer.SUBJECT);
         // Finally we set up the QOP handler if required
         String qop = (String) saslServer.getNegotiatedProperty(Sasl.QOP);
         if ("auth-int".equals(qop) || "auth-conf".equals(qop)) {
            return CompletableFuture.supplyAsync(() -> {
               SaslQopHandler qopHandler = new SaslQopHandler(saslServer);
               ctx.pipeline().addBefore("decoder", "saslQop", qopHandler);
               return allocator -> response(header, MemcachedStatus.NO_ERROR);
            }, ctx.channel().eventLoop());
         }

         // Send the final server challenge
         return CompletableFuture.supplyAsync(() -> {
            ctx.pipeline().replace("decoder", "decoder", new BinaryOpDecoderImpl(server, subject));
            disposeSaslServer();
            return allocator -> response(header, MemcachedStatus.NO_ERROR, serverChallenge == null ? Util.EMPTY_BYTE_ARRAY : serverChallenge);
         }, ctx.channel().eventLoop());
      } catch (Throwable t) {
         return CompletableFuture.failedFuture(new SecurityException(t));
      }
   }

   private void disposeSaslServer() {
      try {
         if (saslServer != null)
            saslServer.dispose();
      } catch (SaslException e) {
         log.debug("Exception while disposing SaslServer", e);
      } finally {
         saslServer = null;
      }
   }
}
