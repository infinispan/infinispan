package org.infinispan.server.core.logging;

import static org.jboss.logging.Messages.getBundle;

import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageBundle;

@MessageBundle(projectCode = "ISPN")
public interface Messages {
   Messages MESSAGES = getBundle(Messages.class);

   @Message(value = "Backup '%s' Created")
   String backupCreated(String name);

   @Message(value = "Backup '%s' Removed")
   String backupRemoved(String name);

   @Message(value = "Backup '%s' Restored")
   String backupRestored(String name);
}
