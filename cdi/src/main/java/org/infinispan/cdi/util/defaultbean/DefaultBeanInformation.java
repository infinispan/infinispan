package org.infinispan.cdi.util.defaultbean;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import java.lang.annotation.Annotation;
import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.util.Set;

import javax.enterprise.util.AnnotationLiteral;

/**
 * We use this annotation as a carrier of qualifiers so that other extensions have access to the original qualifiers of the bean
 * (those removed and replaced by synthetic qualifier by the {@link DefaultBeanExtension}).
 * 
 * @author Jozef Hartinger
 * 
 */
@Target({ TYPE, METHOD, FIELD })
@Retention(RUNTIME)
@Documented
public @interface DefaultBeanInformation {

    @SuppressWarnings("all")
    public static class Literal extends AnnotationLiteral<DefaultBeanInformation> implements DefaultBeanInformation {
        private final Set<Annotation> qualifiers;

        public Literal(Set<Annotation> qualifiers) {
            this.qualifiers = qualifiers;
        }

        public Set<Annotation> getQualifiers() {
            return qualifiers;
        }
    }
}
