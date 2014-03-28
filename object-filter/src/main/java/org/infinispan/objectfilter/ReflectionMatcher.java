package org.infinispan.objectfilter;

import org.infinispan.objectfilter.impl.FilterRegistry;
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
            Class<?> entityClass = clazz;
            for (String propName : path) {

               Class elementType = ReflectionHelper.getElementType(entityClass, propName);
               if (elementType != null) {
                  return true;
               } else {
                  entityClass = ReflectionHelper.getPropertyType(entityClass, propName);
               }

               if (entityClass == null) {
                  //todo [anistor] error?
                  break;
               }
            }
            return false;
         }
      }, clazz.getCanonicalName());
   }
}
