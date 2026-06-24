package org.infinispan.checkstyle.checks.interceptors;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * Checks that if the interceptor handles one read command it handles all of them.
 * <p>
 * {@code visitGetKeyValueCommand} and {@code visitGetCacheEntryCommand} are checked separately by
 * {@link InterceptorGetPairCheck} at error severity because the optimized get interceptor chain requires both.
 */
public class InterceptorDefinesAllReadsCheck extends AbstractInterceptorCheck {
   private static final Set<String> READ_METHODS = new HashSet<>(Arrays.asList(
         "visitGetKeyValueCommand",
         "visitGetCacheEntryCommand",
         "visitGetAllCommand",
         "visitReadOnlyKeyCommand",
         "visitReadOnlyManyCommand"));

   @Override
   protected Set<String> methods() {
      return READ_METHODS;
   }
}
