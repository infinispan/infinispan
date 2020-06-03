package org.infinispan.jboss.marshalling.commons;

import java.io.IOException;

import org.infinispan.commons.configuration.ClassWhiteList;
import org.infinispan.commons.logging.Log;
import org.infinispan.commons.logging.LogFactory;
import org.jboss.marshalling.Unmarshaller;
/*
 * @deprecated since 11.0. To be removed in 14.0 ISPN-11947.
 */
@Deprecated
public final class CheckedClassResolver extends DefaultContextClassResolver {

   protected static final Log log = LogFactory.getLog(CheckedClassResolver.class);

   private final ClassWhiteList classWhiteList;

   public CheckedClassResolver(ClassWhiteList classWhiteList, ClassLoader defaultClassLoader) {
      super(defaultClassLoader);
      this.classWhiteList = classWhiteList;
      classWhiteList.addClasses(JBossExternalizerAdapter.class);
   }

   @Override
   public Class<?> resolveClass(Unmarshaller unmarshaller, String name, long serialVersionUID) throws IOException, ClassNotFoundException {
      boolean safeClass = classWhiteList.isSafeClass(name);
      if (!safeClass)
         throw log.classNotInWhitelist(name);

      return super.resolveClass(unmarshaller, name, serialVersionUID);
   }

}
