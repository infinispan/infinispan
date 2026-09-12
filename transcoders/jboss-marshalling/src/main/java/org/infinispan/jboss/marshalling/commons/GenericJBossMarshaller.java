package org.infinispan.jboss.marshalling.commons;

import org.infinispan.commons.configuration.ClassAllowList;
import org.jboss.marshalling.AnnotationClassExternalizerFactory;

/**
 * A marshaller that makes use of <a href="http://www.jboss.org/jbossmarshalling">JBoss Marshalling</a>
 * to serialize and deserialize objects. This marshaller is oriented at external,
 * non-core Infinispan use, such as the Java Hot Rod client.
 *
 * @author Manik Surtani
 * @version 4.1
 * @see <a href="http://www.jboss.org/jbossmarshalling">JBoss Marshalling</a>
 */
public final class GenericJBossMarshaller extends AbstractJBossMarshaller {

   public GenericJBossMarshaller() {
      this(null, null);
   }

   public GenericJBossMarshaller(ClassLoader classLoader) {
      this(classLoader, null);
   }

   public GenericJBossMarshaller(ClassAllowList classAllowList) {
      this(null, classAllowList);
   }

   public GenericJBossMarshaller(ClassLoader classLoader, ClassAllowList classAllowList) {
      super();
      if (classLoader == null) {
         classLoader = classAllowList != null ? classAllowList.getClassLoader() : null;
      }
      if (classLoader == null) {
         classLoader = Thread.currentThread().getContextClassLoader();
      }
      if (classLoader == null) {
         classLoader = getClass().getClassLoader();
      }
      baseCfg.setClassExternalizerFactory(new AnnotationClassExternalizerFactory());
      baseCfg.setClassResolver(classAllowList == null ?
            new DefaultContextClassResolver(classLoader) :
            new CheckedClassResolver(classAllowList, classLoader)
      );
   }

   @Override
   public void initialize(ClassAllowList classAllowList) {
      ClassLoader classLoader = ((DefaultContextClassResolver) baseCfg.getClassResolver()).getClassLoader();
      baseCfg.setClassResolver(new CheckedClassResolver(classAllowList, classLoader));
   }
}
