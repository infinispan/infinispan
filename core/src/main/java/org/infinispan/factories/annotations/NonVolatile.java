/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2000 - 2008, Red Hat Middleware LLC, and individual contributors
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
package org.infinispan.factories.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * This annotation is used for components that will be registered in the {@link org.infinispan.factories.ComponentRegistry},
 * that are resilient to changes in configuration.  Examples are the {@link org.infinispan.factories.ComponentRegistry}
 * implementation used, which does not change regardless of the configuration.  Components such as the {@link
 * org.infinispan.util.concurrent.locks.LockManager}, though, should <b>never</b> be marked as <tt>@NonVolatile</tt>
 * since based on the configuration, different lock manager implementations may be selected.  LockManager is, hence,
 * <b>not</b> resilient to changes in the configuration.
 * 
 * To sum up: A component is marked as NonVolatile when the component is not dependent on configuration changes that may 
 * happen between create() and start(). This is legacy since from JBoss Cache because in Infinispan there's only start but 
 * no create. 
 *
 * @author Manik Surtani (<a href="mailto:manik@jboss.org">manik@jboss.org</a>)
 * @since 4.0
 */
// ensure this annotation is available at runtime.
@Retention(RetentionPolicy.RUNTIME)

// only applies to classes.
@Target(ElementType.TYPE)
public @interface NonVolatile {
}