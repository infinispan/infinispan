package org.infinispan.checkstyle.checks.interceptors;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class InterceptorDefinesAllReadWritesCheck extends AbstractInterceptorCheck {
   // WriteOnly* commands are not included because these don't ever read the previous value,
   // and therefore there are situations (like loading of data from cache store) where it does not make
   // sense to define them.
   private static final Set<String> WRITE_METHODS = new HashSet<>(Arrays.asList(
         "visitPutKeyValueCommand",
         "visitRemoveCommand",
         "visitReplaceCommand",
         "visitPutMapCommand", // PutMapCommand should load previous values unless IGNORE_RETURN_VALUE flag is set.
         "visitReadWriteKeyValueCommand",
         "visitReadWriteKeyCommand",
         "visitReadWriteManyCommand",
         "visitReadWriteManyEntriesCommand"
   ));

   @Override
   protected Set<String> methods() {
      return WRITE_METHODS;
   }
}
