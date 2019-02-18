package org.infinispan.server.logging;

import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageBundle;

/**
 * @since 10.0
 */
@MessageBundle(projectCode = "ISPN")
public interface Messages {
   Messages MSG = org.jboss.logging.Messages.getBundle(Messages.class);

   @Message(value = "Unrecognized command-line argument '%s'.", id = 90001)
   String unknownParameter(String parameter);

   @Message(value = "Invalid argument '%s'. Arguments must be prefixed with either - or --.", id = 90002)
   String invalidArgument(String command);

   @Message(value = "Invalid argument '%s'. The - prefix must be used only for single-character arguments.", id = 90003)
   String invalidShortArgument(String command);
}
