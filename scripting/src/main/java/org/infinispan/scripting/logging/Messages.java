package org.infinispan.scripting.logging;

import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageBundle;

/**
 * Informational Scripting messages. These start from 21500 so as not to overlap with the logging
 * messages defined in {@link Log} Messages.
 *
 * @author Tristan Tarrant
 * @since 7.0
 */
@MessageBundle(projectCode = "ISPN")
public interface Messages {
   Messages MSG = org.jboss.logging.Messages.getBundle(Messages.class);

   @Message(value = "Executed script  '%s' on cache '%s'", id = 21500)
   String executedScript(String scriptName, String cacheName);
}
