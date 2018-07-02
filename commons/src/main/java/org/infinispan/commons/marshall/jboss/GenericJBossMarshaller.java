package org.infinispan.commons.marshall.jboss;

import java.util.List;

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
      super();
      baseCfg.setClassResolver(
            new DefaultContextClassResolver(this.getClass().getClassLoader()));
   }

   public GenericJBossMarshaller(ClassLoader classLoader) {
      super();
      baseCfg.setClassResolver(
            new DefaultContextClassResolver(classLoader != null ? classLoader : this.getClass().getClassLoader()));
   }

   /**
    * @deprecated Use {@link #GenericJBossMarshaller(ClassWhiteList)} instead
    */
   @Deprecated
   public GenericJBossMarshaller(List<String> whitelist) {
      this(new ClassWhiteList(whitelist));
   }

   public GenericJBossMarshaller(ClassWhiteList classWhiteList) {
      super();
      baseCfg.setClassResolver(
            new CheckedClassResolver(classWhiteList, this.getClass().getClassLoader()));
   }

   public GenericJBossMarshaller(ClassLoader classLoader, ClassWhiteList classWhiteList) {
      super();
      baseCfg.setClassResolver(new CheckedClassResolver(classWhiteList, classLoader));
   }

}
