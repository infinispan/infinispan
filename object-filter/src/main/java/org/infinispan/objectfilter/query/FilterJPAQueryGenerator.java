package org.infinispan.objectfilter.query;

import org.infinispan.query.dsl.impl.JPAQueryGenerator;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
public final class FilterJPAQueryGenerator extends JPAQueryGenerator {

   public FilterJPAQueryGenerator() {
   }

   @Override
   protected String renderEntityName(String rootType) {
      //todo [anistor] this should just check the type can actually be marshalled with current config
      return rootType;
   }
}
