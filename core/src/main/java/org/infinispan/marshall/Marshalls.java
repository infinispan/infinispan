/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2009, Red Hat Middleware LLC, and individual contributors
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
package org.infinispan.marshall;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Once the {@link Externalizer} implementations have been build, it's time to link them up together
 * with the type classes that they externalize and you do so annotating the {@link Externalizer}
 * implementations with {@link Marshalls}.
 *
 * When this annotation is used, it's mandatory that either {@link Marshalls#typeClasses()}
 * or {@link Marshalls#typeClassNames()} is filled up. Basically, both of these properties indicate
 * which classes this Externalizer implementation marshalls. As indicated by the properties, a
 * Externalizer implementation is not limited to marshalling a unique type, on the contrary, it can
 * marshall different types. The difference between {@link Marshalls#typeClasses()} and
 * {@link Marshalls#typeClassNames()} is how these type classes are defined. The first option takes
 * {@link Class} instances, but there might sometimes where the classes your externalizing are private
 * and hence you cannot reference the class instance. In this case, you should use
 * {@link Marshalls#typeClassNames()} and indicate the fully qualified name of the class. You can find
 * an example of this in {@link org.infinispan.marshall.exts.SingletonListExternalizer} that tries to
 * provide a better way to serialize lists with a single element.
 *
 * {@link Marshalls} annotation also takes an positive identifier as optional parameter which is used
 * to identify the type of externalizer. This is used at read time to figure out which {@link Externalizer}
 * should read the contents of the incoming buffer. Using a positive integer allows for very efficient
 * variable length encoding of numbers, and it's much more efficient than shipping {@link Externalizer}
 * implementation class information around. You can use any positive integer as long as it does not clash
 * with any other identifier in the system. You can find information on the pre-assigned identifier ranges
 * in <a href="http://community.jboss.org/docs/DOC-16198">here</a>.
 *
 * It's highly recommended that maintaining of these identifiers is done in a centralized way and you
 * can do so by making annotations reference a set of statically defined identifiers in a separate class
 * or interface. Such class/interface gives a global view of the identifiers in use and so can make it
 * easier to assign new ids.
 *
 * Infinispan does no annotation scanning and so, as indicated in {@link org.infinispan.config.ExternalizerConfig},
 * it is necessary that any Externalizer implementations to be used are listed in the global configuration
 * either via XML or programmatically. When listing these externalizer implementations, users can optionally
 * provide the identifier of the externalizer via XML or programmatically instead of via this annotation.
 * Again, this offers a centralized way to maintain the identifiers but it's important that the rules are clear:
 * An Externalizer implementation, either via XML/programmatic configuration or via annotation, needs to be
 * associated with an identifiers. If it isn't, Infinispan will throw an error and abort startup. If a
 * particular Externalizer implementation defines an id both via XML/programmatic configuration and annotation,
 * the value defined via XML/programmatically is the one that will be used.
 *
 * @author Galder Zamarreño
 * @since 5.0
 */
//ensure this annotation is available at runtime.
@Retention(RetentionPolicy.RUNTIME)

// only applies to classes.
@Target(ElementType.TYPE) 
public @interface Marshalls {
   
   Class[] typeClasses() default {};

   String[] typeClassNames() default {};
   
   int id() default Integer.MAX_VALUE;
   
}
