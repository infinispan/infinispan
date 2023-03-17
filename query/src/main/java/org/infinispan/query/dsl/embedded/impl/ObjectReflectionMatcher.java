package org.infinispan.query.dsl.embedded.impl;

import org.infinispan.AdvancedCache;
import org.infinispan.objectfilter.impl.MetadataAdapter;
import org.infinispan.objectfilter.impl.ReflectionMatcher;
import org.infinispan.objectfilter.impl.syntax.parser.EntityNameResolver;
import org.infinispan.objectfilter.impl.util.ReflectionHelper;
import org.infinispan.search.mapper.mapping.SearchMapping;

/**
 * Like ReflectionMatcher but also able to use Hibernate Search metadata if available.
 *
 * @author anistor@redhat.com
 * @since 10.1
 */
public final class ObjectReflectionMatcher extends ReflectionMatcher {

   private final AdvancedCache<?, ?> cache;

   public static ObjectReflectionMatcher create(AdvancedCache<?, ?> cache, EntityNameResolver<Class<?>> entityNameResolv,
                                                SearchMapping searchMapping) {
      return searchMapping == null ? new ObjectReflectionMatcher(cache, entityNameResolv) :
            new ObjectReflectionMatcher(cache, new HibernateSearchPropertyHelper(searchMapping, entityNameResolv));
   }

   private ObjectReflectionMatcher(AdvancedCache<?, ?> cache, HibernateSearchPropertyHelper hibernateSearchPropertyHelper) {
      super(hibernateSearchPropertyHelper);
      this.cache = cache;
   }

   private ObjectReflectionMatcher(AdvancedCache<?, ?> cache, EntityNameResolver<Class<?>> entityNameResolver) {
      super(entityNameResolver);
      this.cache = cache;
   }

   @Override
   protected MetadataAdapter<Class<?>, ReflectionHelper.PropertyAccessor, String> createMetadataAdapter(Class<?> clazz) {
      return new ReflectionMetadataProjectableAdapter(super.createMetadataAdapter(clazz), cache);
   }
}
