package org.infinispan.query.dsl.embedded.impl;

import org.infinispan.query.objectfilter.impl.MetadataAdapter;
import org.infinispan.query.objectfilter.impl.ReflectionMatcher;
import org.infinispan.query.objectfilter.impl.syntax.parser.EntityNameResolver;
import org.infinispan.query.objectfilter.impl.util.ReflectionHelper;
import org.infinispan.search.mapper.mapping.SearchMapping;

/**
 * Like ReflectionMatcher but also able to use Hibernate Search metadata if available.
 *
 * @author anistor@redhat.com
 * @since 10.1
 */
public final class ObjectReflectionMatcher extends ReflectionMatcher {

   public static ObjectReflectionMatcher create(EntityNameResolver<Class<?>> entityNameResolver,
                                                SearchMapping searchMapping) {
      return searchMapping == null ? new ObjectReflectionMatcher(entityNameResolver) :
            new ObjectReflectionMatcher(new HibernateSearchPropertyHelper(searchMapping, entityNameResolver));
   }

   private ObjectReflectionMatcher(HibernateSearchPropertyHelper hibernateSearchPropertyHelper) {
      super(hibernateSearchPropertyHelper);
   }

   private ObjectReflectionMatcher(EntityNameResolver<Class<?>> entityNameResolver) {
      super(entityNameResolver);
   }

   @Override
   protected MetadataAdapter<Class<?>, ReflectionHelper.PropertyAccessor, String> createMetadataAdapter(Class<?> clazz) {
      return new ReflectionMetadataProjectableAdapter(super.createMetadataAdapter(clazz));
   }
}
