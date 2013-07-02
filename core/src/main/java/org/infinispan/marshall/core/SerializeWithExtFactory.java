package org.infinispan.marshall.core;

import org.infinispan.commons.marshall.SerializeWith;
import org.infinispan.commons.marshall.jboss.JBossExternalizerAdapter;
import org.infinispan.marshall.LegacyExternalizerAdapter;
import org.jboss.marshalling.AnnotationClassExternalizerFactory;
import org.jboss.marshalling.ClassExternalizerFactory;
import org.jboss.marshalling.Externalizer;

/**
 * JBoss Marshalling plugin class for {@link ClassExternalizerFactory} that allows for Infinispan
 * annotations to be used instead of JBoss Marshalling ones in order to discover which classes are
 * serializable with Infinispan externalizers.
 *
 * @author Galder Zamarreño
 * @author Tristan Tarrant
 * @since 6.0
 */
public class SerializeWithExtFactory implements ClassExternalizerFactory {

   final ClassExternalizerFactory jbmarExtFactory = new AnnotationClassExternalizerFactory();

   @Override
   public Externalizer getExternalizer(Class<?> type) {
      try {
         final org.infinispan.commons.marshall.Externalizer<?> ext;
         SerializeWith ann = type.getAnnotation(SerializeWith.class);
         if (ann != null) {
            ext = ann.value().newInstance();
         } else {
            org.infinispan.marshall.SerializeWith legacy = type.getAnnotation(org.infinispan.marshall.SerializeWith.class);
            if (legacy != null) {
               ext = new LegacyExternalizerAdapter(legacy.value().newInstance());
            } else {
               // Check for JBoss Marshaller's @Externalize
               return jbmarExtFactory.getExternalizer(type);
            }
         }
         return new JBossExternalizerAdapter(ext);
      } catch (Exception e) {
         throw new IllegalArgumentException(String.format("Cannot instantiate externalizer for %s", type), e);
      }

   }
}
