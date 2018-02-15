package org.infinispan.commons.marshall.jboss;

import org.infinispan.commons.marshall.MarshallUtil;
import org.jboss.marshalling.Unmarshaller;

import java.io.IOException;
import java.util.List;

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

   public GenericJBossMarshaller(List<String> whitelist) {
      super();
      baseCfg.setClassResolver(
         new CheckedClassResolver(whitelist, this.getClass().getClassLoader()));
   }

   private static final class CheckedClassResolver extends DefaultContextClassResolver {

      private final List<String> whitelist;

      CheckedClassResolver(List<String> whitelist, ClassLoader defaultClassLoader) {
         super(defaultClassLoader);
         this.whitelist = whitelist;
      }

      @Override
      public Class<?> resolveClass(Unmarshaller unmarshaller, String name, long serialVersionUID) throws IOException, ClassNotFoundException {
         boolean safeClass = MarshallUtil.isSafeClass(name, whitelist);
         if (!safeClass)
            throw log.classNotInWhitelist(name);

         return super.resolveClass(unmarshaller, name, serialVersionUID);
      }

   }

}
