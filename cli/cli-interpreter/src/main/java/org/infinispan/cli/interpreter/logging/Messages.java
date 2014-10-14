package org.infinispan.cli.interpreter.logging;

import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageBundle;

/**
 * Informational CLI messages. These start from 19500 so as not to overlap with the logging messages defined in {@link Log}
 * Messages.
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
@MessageBundle(projectCode = "ISPN")
public interface Messages {
   Messages MSG = org.jboss.logging.Messages.getBundle(Messages.class);

   @Message(value="Synchronized %d entries using migrator '%s' on cache '%s'", id=19500)
   String synchronizedEntries(long count, String cacheName, String migrator);

   @Message(value="Disconnected '%s' migrator source on cache '%s'", id=19501)
   String disonnectedSource(String migratorName, String cacheNname);

   @Message(value="Dumped keys for cache %s", id=19502)
   String dumpedKeys(String cacheName);
}
