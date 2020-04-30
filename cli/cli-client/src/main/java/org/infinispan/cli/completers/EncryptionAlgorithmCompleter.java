package org.infinispan.cli.completers;

import java.util.Collection;

import org.infinispan.cli.Context;
import org.infinispan.cli.user.UserTool;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class EncryptionAlgorithmCompleter extends ListCompleter {

   @Override
   Collection<String> getAvailableItems(Context context) {
      return UserTool.DEFAULT_ALGORITHMS;
   }
}
