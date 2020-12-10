package org.infinispan.cli.completers;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

import org.infinispan.cli.Context;

/**
 * A {@link org.aesh.command.completer.OptionCompleter} for protocol connectors.
 *
 * @author Tristan Tarrant
 * @since 12.1
 */
public class ConnectorCompleter extends ListCompleter {
   @Override
   Collection<String> getAvailableItems(Context context) throws IOException {
      try {
         return context.getConnection().getConnectorNames();
      } catch (IOException e) {
         return Collections.emptyList();
      }
   }
}
