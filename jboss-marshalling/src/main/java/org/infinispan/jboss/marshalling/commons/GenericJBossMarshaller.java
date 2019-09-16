package org.infinispan.jboss.marshalling.commons;

import org.infinispan.commons.configuration.ClassWhiteList;

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

   public GenericJBossMarshaller(ClassWhiteList classWhiteList) {
      this(null, classWhiteList);
   }

   public GenericJBossMarshaller(ClassLoader classLoader, ClassWhiteList classWhiteList) {
      super();
      if (classLoader == null) {
         classLoader = getClass().getClassLoader();
      }
      baseCfg.setClassResolver(classWhiteList == null ?
            new DefaultContextClassResolver(classLoader) :
            new CheckedClassResolver(classWhiteList, classLoader)
      );
   }

   @Override
   public void initialize(ClassWhiteList classWhiteList) {
      ClassLoader classLoader = ((DefaultContextClassResolver) baseCfg.getClassResolver()).getClassLoader();
      baseCfg.setClassResolver(new CheckedClassResolver(classWhiteList, classLoader));
   }
}
