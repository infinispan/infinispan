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
   String unknownArgument(String argument);

   @Message(value = "Invalid argument '%s'. Arguments must be prefixed with either - or --.", id = 90002)
   String invalidArgument(String argument);

   @Message(value = "Invalid argument '%s'. The - prefix must be used only for single-character arguments.", id = 90003)
   String invalidShortArgument(String command);

   @Message(value = "Username: ")
   String userToolUsername();

   @Message(value = "Password: ")
   String userToolPassword();

   @Message(value = "User '%s' already exists. Overwrite (y/n) ? ")
   String userToolUserExists(String username);

   @Message(value = "Cannot add user '%s' without a password.\n")
   String userToolNoPassword(String username);
}
