package org.infinispan.server.configuration.security;

import org.infinispan.commons.configuration.Builder;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 13.0
 **/
public interface RealmProviderBuilder<T extends RealmProvider> extends Builder<T>, Comparable<RealmProviderBuilder> {
   int SORT_FIRST = -10;
   int SORT_DEFAULT = 0;
   int SORT_PENULTIMATE = 10;
   int SORT_LAST = 20;

   String name();

   default int sortOrder() {
      return SORT_DEFAULT;
   }

   @Override
   default int compareTo(RealmProviderBuilder o) {
      return Integer.compare(sortOrder(), o.sortOrder());
   }
}
