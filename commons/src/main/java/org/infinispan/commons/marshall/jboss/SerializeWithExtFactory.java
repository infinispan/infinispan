package org.infinispan.commons.marshall.jboss;

import org.infinispan.commons.marshall.SerializeWith;
import org.jboss.marshalling.AnnotationClassExternalizerFactory;
import org.jboss.marshalling.ClassExternalizerFactory;
import org.jboss.marshalling.Externalizer;

/**
 * JBoss Marshalling plugin class for {@link ClassExternalizerFactory} that
 * allows for Infinispan annotations to be used instead of JBoss Marshalling
 * ones in order to discover which classes are serializable with Infinispan
 * externalizers.
 *
 * @author Galder Zamarreño
 * @since 5.0
 */
public class SerializeWithExtFactory implements ClassExternalizerFactory {

   final ClassExternalizerFactory jbmarExtFactory = new AnnotationClassExternalizerFactory();

   @Override
   public Externalizer getExternalizer(Class<?> type) {
      SerializeWith ann = type.getAnnotation(SerializeWith.class);
      if (ann == null) {
         // Check for JBoss Marshaller's @Externalize
         return jbmarExtFactory.getExternalizer(type);
      } else {
         try {
            return new JBossExternalizerAdapter(ann.value().newInstance());
         } catch (Exception e) {
            throw new IllegalArgumentException(String.format(
                  "Cannot instantiate externalizer for %s", type), e);
         }
      }
   }
}
