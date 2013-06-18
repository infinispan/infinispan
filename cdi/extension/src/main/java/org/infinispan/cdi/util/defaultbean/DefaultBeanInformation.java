/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2010, Red Hat, Inc., and individual contributors
 * as indicated by the @author tags. See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
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
