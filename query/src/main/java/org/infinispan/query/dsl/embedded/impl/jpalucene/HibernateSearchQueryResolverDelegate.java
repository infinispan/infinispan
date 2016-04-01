package org.infinispan.query.dsl.embedded.impl.jpalucene;

import org.hibernate.hql.ast.origin.hql.resolve.path.PathedPropertyReferenceSource;
import org.hibernate.hql.ast.spi.EntityNamesResolver;
import org.infinispan.objectfilter.impl.hql.FilterQueryResolverDelegate;
import org.infinispan.objectfilter.impl.hql.FilterTypeDescriptor;
import org.infinispan.query.logging.Log;
import org.jboss.logging.Logger;

import java.util.List;

/**
 * @author anistor@redhat.com
 * @since 9.0
 */
public class HibernateSearchQueryResolverDelegate extends FilterQueryResolverDelegate {

   private static final Log log = Logger.getMessageLogger(Log.class, HibernateSearchQueryResolverDelegate.class.getName());

   public HibernateSearchQueryResolverDelegate(EntityNamesResolver entityNamesResolver, HibernateSearchPropertyHelper propertyHelper) {
      super(entityNamesResolver, propertyHelper);
   }

   @Override
   protected PathedPropertyReferenceSource normalizeProperty(FilterTypeDescriptor type, List<String> path, String propertyName) {
      PathedPropertyReferenceSource pathedPropertyReferenceSource = super.normalizeProperty(type, path, propertyName);
      if (status != Status.DEFINING_SELECT && !type.hasEmbeddedProperty(propertyName)) {
         HibernateSearchPropertyHelper propertyHelper = (HibernateSearchPropertyHelper) this.propertyHelper;
         if (propertyHelper.hasAnalyzedProperty(type.getEntityType(), type.makeJoinedPath(propertyName))) {
            throw log.getQueryOnAnalyzedPropertyNotSupportedException(type.getEntityType(), propertyName);
         }
      }
      return pathedPropertyReferenceSource;
   }
}
