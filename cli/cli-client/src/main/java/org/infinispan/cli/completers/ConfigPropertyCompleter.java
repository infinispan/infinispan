package org.infinispan.cli.completers;

import java.util.Collection;

import org.infinispan.cli.Context;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 12.0
 **/
public class ConfigPropertyCompleter extends ListCompleter {

   @Override
   Collection<String> getAvailableItems(Context context) {
      return Context.Property.NAMES;
   }
}
