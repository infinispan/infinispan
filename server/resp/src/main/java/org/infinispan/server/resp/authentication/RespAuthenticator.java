package org.infinispan.server.resp.authentication;

import java.util.concurrent.CompletionStage;

import javax.security.auth.Subject;
import javax.security.sasl.SaslException;

import io.netty.channel.Channel;

public interface RespAuthenticator {

   CompletionStage<Subject> clientCertAuth(Channel channel) throws SaslException;

   CompletionStage<Subject> usernamePasswordAuth(String username, char[] password);

   boolean isClientCertAuthEnabled();
}
