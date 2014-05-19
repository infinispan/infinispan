package org.infinispan.objectfilter.impl;

import org.infinispan.objectfilter.impl.hql.FilterProcessingChain;
import org.infinispan.objectfilter.impl.hql.ReflectionPropertyHelper;
import org.infinispan.objectfilter.impl.predicateindex.MatcherEvalContext;
import org.infinispan.objectfilter.impl.predicateindex.ReflectionMatcherEvalContext;
import org.infinispan.objectfilter.impl.predicateindex.be.BETreeMaker;
import org.infinispan.objectfilter.impl.util.ReflectionHelper;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
public final class ReflectionMatcher extends BaseMatcher<Class<?>, String> {

   private final ClassLoader classLoader;

   public ReflectionMatcher(ClassLoader classLoader) {
      this.classLoader = classLoader;
   }

   @Override
   protected MatcherEvalContext<String> startContext(Object instance, Set<String> knownTypes) {
      String typeName = instance.getClass().getCanonicalName();
      if (knownTypes.contains(typeName)) {
         return new ReflectionMatcherEvalContext(instance);
      } else {
         return null;
      }
   }

   @Override
   protected FilterProcessingChain<?> createFilterProcessingChain(Map<String, Object> namedParameters) {
      return FilterProcessingChain.build(new ReflectionPropertyHelper(classLoader), namedParameters);
   }

   @Override
   protected FilterRegistry<String> createFilterRegistryForType(final Class<?> clazz) {
      return new FilterRegistry<String>(new BETreeMaker.AttributePathTranslator<String>() {
         @Override
         public List<String> translatePath(List<String> path) {
            return path;
         }

         @Override
         public boolean isRepeated(List<String> path) {
            ReflectionHelper.PropertyAccessor a = ReflectionHelper.getAccessor(clazz, path.get(0));
            if (a.isMultiple()) {
               return true;
            }
            for (int i = 1; i < path.size(); i++) {
               a = a.getAccessor(path.get(i));
               if (a.isMultiple()) {
                  return true;
               }
            }
            return false;
         }
      }, clazz.getCanonicalName());
   }
}
