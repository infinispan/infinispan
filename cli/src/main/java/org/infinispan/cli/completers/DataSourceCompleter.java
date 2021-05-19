package org.infinispan.cli.completers;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

import org.infinispan.cli.Context;

/**
 * A {@link org.aesh.command.completer.OptionCompleter} for data sources.
 *
 * @author Tristan Tarrant
 * @since 13.0
 */
public class DataSourceCompleter extends ListCompleter {
   @Override
   Collection<String> getAvailableItems(Context context) throws IOException {
      try {
         return context.getConnection().getDataSourceNames();
      } catch (IOException e) {
         return Collections.emptyList();
      }
   }
}
