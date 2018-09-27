package org.infinispan.query.remote.impl;

import org.infinispan.AdvancedCache;
import org.infinispan.commons.dataconversion.IdentityEncoder;
import org.infinispan.objectfilter.Matcher;

/**
 * @author anistor@redhat.com
 * @since 9.0
 */
final class ObjectRemoteQueryEngine extends BaseRemoteQueryEngine {

   ObjectRemoteQueryEngine(AdvancedCache<?, ?> cache, Class<? extends Matcher> matcherImplClass, boolean isIndexed) {
      super(cache.withEncoding(IdentityEncoder.class), isIndexed, matcherImplClass, null);
   }
}
