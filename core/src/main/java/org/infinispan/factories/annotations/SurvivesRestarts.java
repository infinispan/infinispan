/*
 * JBoss, Home of Professional Open Source
 * Copyright 2009 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
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
 * that are meant to be retained in the component registry even after the component registry is stopped.  Components
 * annotated as such would not have to be recreated and rewired between stopping and starting a component registry.
 * <br />
 * As a rule of thumb though, use of this annotation on components should be avoided, since resilient components are
 * retained even after a component registry is stopped and consume resources.  Only components necessary for and critical
 * to bootstrapping and restarting the component registry should be annotated as such.
 *
 * @author Manik Surtani (<a href="mailto:manik@jboss.org">manik@jboss.org</a>)
 * @since 4.0
 */
// ensure this annotation is available at runtime.
@Retention(RetentionPolicy.RUNTIME)

// only applies to classes.
@Target(ElementType.TYPE)
public @interface SurvivesRestarts {
}