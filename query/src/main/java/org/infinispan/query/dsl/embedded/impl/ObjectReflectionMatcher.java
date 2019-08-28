package org.infinispan.query.dsl.embedded.impl;

import org.hibernate.search.spi.SearchIntegrator;
import org.infinispan.objectfilter.impl.ReflectionMatcher;
import org.infinispan.objectfilter.impl.syntax.parser.EntityNameResolver;

/**
 * Like ReflectionMatcher but also able to use Hibernate Search metadata if available.
 *
 * @author anistor@redhat.com
 * @since 10.1
 */
public final class ObjectReflectionMatcher extends ReflectionMatcher {

   private ObjectReflectionMatcher(HibernateSearchPropertyHelper hibernateSearchPropertyHelper) {
      super(hibernateSearchPropertyHelper);
   }

   private ObjectReflectionMatcher(EntityNameResolver entityNameResolver) {
      super(entityNameResolver);
   }

   public static ObjectReflectionMatcher create(EntityNameResolver entityNameResolver, SearchIntegrator searchIntegrator) {
      return searchIntegrator == null ? new ObjectReflectionMatcher(entityNameResolver) :
            new ObjectReflectionMatcher(new HibernateSearchPropertyHelper(searchIntegrator, entityNameResolver));
   }
}
