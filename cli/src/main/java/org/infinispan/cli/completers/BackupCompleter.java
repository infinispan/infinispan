package org.infinispan.cli.completers;

import java.io.IOException;
import java.util.Collection;

import org.infinispan.cli.Context;

/**
 * A {@link org.aesh.command.completer.OptionCompleter} for Backup files that are available on the server.
 *
 * @author Ryan Emerson
 * @since 12.0
 */
public class BackupCompleter extends ListCompleter {
   @Override
   Collection<String> getAvailableItems(Context context) throws IOException {
      return context.getConnection().getBackupNames();
   }
}
