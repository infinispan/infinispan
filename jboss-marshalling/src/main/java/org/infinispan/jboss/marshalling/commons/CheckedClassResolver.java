package org.infinispan.jboss.marshalling.commons;

import java.io.IOException;

import org.infinispan.commons.configuration.ClassAllowList;
import org.infinispan.commons.logging.Log;
import org.infinispan.commons.logging.LogFactory;
import org.jboss.marshalling.Unmarshaller;
public final class CheckedClassResolver extends DefaultContextClassResolver {

   protected static final Log log = LogFactory.getLog(CheckedClassResolver.class);

   private final ClassAllowList classAllowList;

   public CheckedClassResolver(ClassAllowList classAllowList, ClassLoader defaultClassLoader) {
      super(defaultClassLoader);
      this.classAllowList = classAllowList;
   }

   @Override
   public Class<?> resolveClass(Unmarshaller unmarshaller, String name, long serialVersionUID) throws IOException, ClassNotFoundException {
      boolean safeClass = classAllowList.isSafeClass(name);
      if (!safeClass)
         throw log.classNotInAllowList(name);

      return super.resolveClass(unmarshaller, name, serialVersionUID);
   }

}
