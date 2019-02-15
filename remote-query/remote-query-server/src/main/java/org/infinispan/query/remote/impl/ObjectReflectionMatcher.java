package org.infinispan.query.remote.impl;

import org.hibernate.search.spi.SearchIntegrator;
import org.infinispan.objectfilter.impl.ReflectionMatcher;
import org.infinispan.objectfilter.impl.syntax.parser.EntityNameResolver;
import org.infinispan.query.dsl.embedded.impl.HibernateSearchPropertyHelper;

/**
 * @since 9.4
 */
final class ObjectReflectionMatcher extends ReflectionMatcher {

   private ObjectReflectionMatcher(HibernateSearchPropertyHelper hibernateSearchPropertyHelper) {
      super(hibernateSearchPropertyHelper);
   }

   private ObjectReflectionMatcher(EntityNameResolver entityNameResolver) {
      super(entityNameResolver);
   }

   static ObjectReflectionMatcher create(EntityNameResolver entityNameResolver, SearchIntegrator searchIntegrator, ClassLoader classLoader) {
      if (searchIntegrator == null) return new ObjectReflectionMatcher(entityNameResolver);
      return new ObjectReflectionMatcher(new HibernateSearchPropertyHelper(searchIntegrator, entityNameResolver, classLoader));
   }

}
