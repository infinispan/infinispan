package org.infinispan.cdi.common.util.defaultbean;

import javax.enterprise.util.AnnotationLiteral;
import java.lang.annotation.Annotation;
import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.util.Set;

import static java.lang.annotation.ElementType.*;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

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
