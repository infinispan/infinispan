package org.infinispan.query.remote.impl;

import org.infinispan.objectfilter.impl.ReflectionMatcher;
import org.infinispan.objectfilter.impl.syntax.parser.EntityNameResolver;
import org.infinispan.query.dsl.embedded.impl.HibernateSearchPropertyHelper;

/**
 * @since 9.4
 */
final class ObjectReflectionMatcher extends ReflectionMatcher {

   ObjectReflectionMatcher(HibernateSearchPropertyHelper hibernateSearchPropertyHelper) {
      super(hibernateSearchPropertyHelper);
   }

   ObjectReflectionMatcher(EntityNameResolver entityNameResolver) {
      super(entityNameResolver);
   }
}
