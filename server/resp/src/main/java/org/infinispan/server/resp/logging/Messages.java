package org.infinispan.server.resp.logging;

import static org.jboss.logging.Messages.getBundle;

import java.lang.invoke.MethodHandles;

import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageBundle;

@MessageBundle(projectCode = "ISPN")
public interface Messages {
   Messages MESSAGES = getBundle(MethodHandles.lookup(), Messages.class);

   @Message(value = "-NOPROTO sorry this protocol version is not supported")
   String unsupportedProtocol();

   @Message(value = "NOAUTH HELLO must be called with the client already authenticated, otherwise the HELLO <proto> AUTH <user> <pass> option can be used to authenticate the client and select the RESP protocol version at the same time")
   String noAuthHello();

   @Message(value = "Lua engine is not active")
   String scriptEngineDisabled();

   @Message(value = "Invalid bitfield type. Use something like i16 u8. Note that u64 is not supported but i64 is.")
   String invalidBitfieldType();

   @Message(value = "bit offset is not an integer or out of range")
   String invalidBitOffset();
}
