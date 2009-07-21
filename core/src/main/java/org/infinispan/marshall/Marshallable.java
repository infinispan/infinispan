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
 * This annotation is used for those classes that need to be marshalled/unmarshalled between nodes
 * in the cluster or to/from cahe stores. Such classes need to provide an implementation for 
 * {@link Externalizer} interface and a unique index number (see {@link Ids} for index numbers 
 * currently allocated).
 * 
 * @author Galder Zamarreño
 * @since 4.0
 */
//ensure this annotation is available at runtime.
@Retention(RetentionPolicy.RUNTIME)

// only applies to classes.
@Target(ElementType.TYPE) 
public @interface Marshallable {
   
   Class<? extends Externalizer> externalizer() default Externalizer.class;
   
   int id();
   
}
