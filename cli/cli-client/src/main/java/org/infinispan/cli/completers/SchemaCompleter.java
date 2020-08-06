package org.infinispan.cli.completers;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

import org.infinispan.cli.Context;
import org.infinispan.cli.connection.Connection;
import org.infinispan.cli.resources.ContainerResource;

/**
 * A {@link org.aesh.command.completer.OptionCompleter} for proto schemas that are available on the server.
 *
 * @author Ryan Emerson
 * @since 12.0
 */
public class SchemaCompleter extends ListCompleter {
   @Override
   Collection<String> getAvailableItems(Context context) throws IOException {
      Connection connection = context.getConnection();
      ContainerResource container = connection.getActiveResource().findAncestor(ContainerResource.class);
      return container != null ? connection.getAvailableSchemas(container.getName()) : Collections.emptyList();
   }
}
