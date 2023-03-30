package org.infinispan.server.memcached.binary;

import static org.infinispan.server.memcached.binary.BinaryConstants.MEMCACHED_SASL_PROTOCOL;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import javax.security.auth.Subject;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;

import org.infinispan.commons.util.Util;
import org.infinispan.server.core.security.sasl.SaslAuthenticator;
import org.infinispan.server.core.security.sasl.SubjectSaslServer;
import org.infinispan.server.core.transport.SaslQopHandler;
import org.infinispan.server.memcached.MemcachedServer;
import org.infinispan.server.memcached.MemcachedStatus;

import io.netty.buffer.ByteBuf;

abstract class BinaryAuthDecoder extends BinaryDecoder {
   private SaslServer saslServer;

   protected BinaryAuthDecoder(MemcachedServer server) {
      super(server, ANONYMOUS);
   }

   protected void saslListMechs(BinaryHeader header) {
      byte[] mechs = String.join(" ", server.getConfiguration().authentication().sasl().mechanisms()).getBytes(StandardCharsets.US_ASCII);
      ByteBuf r = response(header, MemcachedStatus.NO_ERROR, mechs);
      send(header, CompletableFuture.completedFuture(r));
   }

   protected void saslAuth(BinaryHeader header, byte[] mech, byte[] data) {
      CompletionStage<ByteBuf> r = server.getBlockingManager().supplyBlocking(() -> {
         try {
            saslServer = SaslAuthenticator.createSaslServer(server.getConfiguration().authentication().sasl(), channel, new String(mech, StandardCharsets.US_ASCII), MEMCACHED_SASL_PROTOCOL);
            return doSasl(header, data);
         } catch (Throwable t) {
            return response(header, MemcachedStatus.AUTHN_ERROR, t);
         }
      }, "memcached-sasl-auth");
      send(header, r);
   }

   protected void saslStep(BinaryHeader header, byte[] mech, byte[] data) {
      CompletionStage<ByteBuf> r = server.getBlockingManager().supplyBlocking(() -> doSasl(header, data), "memcached-sasl-step");
      send(header, r);
   }

   private ByteBuf doSasl(BinaryHeader header, byte[] data) {
      try {
         byte[] serverChallenge = saslServer.evaluateResponse(data);
         if (saslServer.isComplete()) {
            // Obtaining the subject might be expensive, so do it before sending the final server challenge, otherwise the
            // client might send another operation before this is complete
            Subject subject = (Subject) saslServer.getNegotiatedProperty(SubjectSaslServer.SUBJECT);
            // Finally we set up the QOP handler if required
            String qop = (String) saslServer.getNegotiatedProperty(Sasl.QOP);
            if ("auth-int".equals(qop) || "auth-conf".equals(qop)) {
               channel.eventLoop().submit(() -> {
                  channel.writeAndFlush(response(header, MemcachedStatus.NO_ERROR));
                  SaslQopHandler qopHandler = new SaslQopHandler(saslServer);
                  channel.pipeline().addBefore("decoder", "saslQop", qopHandler);
               });
            } else {
               // Send the final server challenge
               channel.eventLoop().submit(() -> {
                  channel.writeAndFlush(response(header, MemcachedStatus.NO_ERROR, serverChallenge == null ? Util.EMPTY_BYTE_ARRAY : serverChallenge));
                  channel.pipeline().replace("decoder", "decoder", new BinaryOpDecoderImpl(server, subject));
                  disposeSaslServer();
               });
            }
            return null;
         } else {
            return response(header, MemcachedStatus.AUTHN_CONTINUE, serverChallenge);
         }
      } catch (Throwable t) {
         return response(header, MemcachedStatus.AUTHN_ERROR, t);
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
