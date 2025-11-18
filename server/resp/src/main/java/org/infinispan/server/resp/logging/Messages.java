package org.infinispan.server.resp.logging;

import static org.jboss.logging.Messages.getBundle;

import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageBundle;

@MessageBundle(projectCode = "ISPN")
public interface Messages {
   Messages MESSAGES = getBundle(Messages.class);

   @Message(value = "-NOPROTO sorry this protocol version is not supported")
   String unsupportedProtocol();

   @Message(value = "NOAUTH HELLO must be called with the client already authenticated, otherwise the HELLO <proto> AUTH <user> <pass> option can be used to authenticate the client and select the RESP protocol version at the same time")
   String noAuthHello();

}
