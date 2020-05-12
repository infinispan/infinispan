package org.infinispan.query.dsl.embedded.impl;

import org.infinispan.objectfilter.impl.ReflectionMatcher;
import org.infinispan.objectfilter.impl.syntax.parser.EntityNameResolver;
import org.infinispan.search.mapper.mapping.SearchMappingHolder;

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

   private ObjectReflectionMatcher(EntityNameResolver<Class<?>> entityNameResolver) {
      super(entityNameResolver);
   }

   public static ObjectReflectionMatcher create(EntityNameResolver<Class<?>> entityNameResolver, SearchMappingHolder searchMapping) {
      return searchMapping == null ? new ObjectReflectionMatcher(entityNameResolver) :
            new ObjectReflectionMatcher(new HibernateSearchPropertyHelper(searchMapping, entityNameResolver));
   }
}
