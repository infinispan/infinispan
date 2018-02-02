package org.infinispan.client.hotrod.impl.transport.netty;

import static io.netty.util.internal.EmptyArrays.EMPTY_BYTES;

import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Function;

import javax.security.auth.Subject;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslException;

import org.infinispan.client.hotrod.configuration.AuthenticationConfiguration;
import org.infinispan.client.hotrod.impl.operations.OperationsFactory;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;

class AuthHandler extends ActivationHandler {
   private static final Log log = LogFactory.getLog(AuthHandler.class);
   private static final boolean trace = log.isTraceEnabled();

   private static final String AUTH_INT = "auth-int";
   private static final String AUTH_CONF = "auth-conf";

   static final String NAME = "auth-handler";

   private final AuthenticationConfiguration authentication;
   private final SaslClient saslClient;
   private final OperationsFactory operationsFactory;

   AuthHandler(AuthenticationConfiguration authentication, SaslClient saslClient,
                      OperationsFactory operationsFactory) {
      this.authentication = authentication;
      this.saslClient = saslClient;
      this.operationsFactory = operationsFactory;
   }

   @Override
   public void channelActive(ChannelHandlerContext ctx) throws Exception {
      Channel channel = ctx.channel();
      operationsFactory.newAuthMechListOperation(channel).execute().thenCompose(serverMechs -> {
         if (!serverMechs.contains(authentication.saslMechanism())) {
            throw log.unsupportedMech(authentication.saslMechanism(), serverMechs);
         }
         if (trace) {
            log.tracef("Authenticating using mech: %s", authentication.saslMechanism());
         }
         byte response[];
         if (saslClient.hasInitialResponse()) {
            try {
               response = evaluateChallenge(saslClient, EMPTY_BYTES, authentication.clientSubject());
            } catch (SaslException e) {
               throw new CompletionException(e);
            }
         } else {
            response = EMPTY_BYTES;
         }

         return operationsFactory.newAuthOperation(channel, authentication.saslMechanism(), response).execute();
      }).thenCompose(new ChallengeEvaluator(channel, saslClient)).thenRun(() -> {
         String qop = (String) saslClient.getNegotiatedProperty(Sasl.QOP);
         if (qop != null && (qop.equalsIgnoreCase(AUTH_INT) || qop.equalsIgnoreCase(AUTH_CONF))) {
            channel.pipeline().addFirst(
                  new LengthFieldBasedFrameDecoder(Integer.MAX_VALUE, 0, 4, 0, 4),
                  new SaslDecoderEncoder(saslClient));
         } else {
            try {
               saslClient.dispose();
            } catch (SaslException e) {
               channel.pipeline().fireExceptionCaught(e);
            }
         }
         channel.pipeline().remove(this);
         channel.pipeline().fireUserEventTriggered(ActivationHandler.ACTIVATION_EVENT);
      }).exceptionally(throwable -> {
         while (throwable instanceof CompletionException && throwable.getCause() != null) {
            throwable = throwable.getCause();
         }
         channel.pipeline().fireExceptionCaught(throwable);
         return null;
      });
   }

   private byte[] evaluateChallenge(final SaslClient saslClient, final byte[] challenge, Subject clientSubject) throws SaslException {
      if(clientSubject != null) {
         try {
            return Subject.doAs(clientSubject,
                  (PrivilegedExceptionAction<byte[]>) () -> saslClient.evaluateChallenge(challenge));
         } catch (PrivilegedActionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof SaslException) {
               throw (SaslException)cause;
            } else {
               throw new RuntimeException(cause);
            }
         }
      } else {
         return saslClient.evaluateChallenge(challenge);
      }
   }

   private class ChallengeEvaluator implements Function<byte[], CompletableFuture<byte[]>> {
      private final Channel channel;
      private final SaslClient saslClient;

      private ChallengeEvaluator(Channel channel, SaslClient saslClient) {
         this.channel = channel;
         this.saslClient = saslClient;
      }

      @Override
      public CompletableFuture<byte[]> apply(byte[] challenge) {
         if (!saslClient.isComplete() && challenge != null) {
            byte[] response;
            try {
               response = evaluateChallenge(saslClient, challenge, authentication.clientSubject());
            } catch (SaslException e) {
               throw new CompletionException(e);
            }
            if (response != null) {
               return operationsFactory.newAuthOperation(channel, authentication.saslMechanism(), response)
                     .execute().thenCompose(this);
            }
         }
         return CompletableFuture.completedFuture(null);
      }
   }
}
