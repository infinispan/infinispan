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
package org.infinispan.lifecycle;

import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.GlobalComponentRegistry;

/**
 * ModuleLifecycle is an internal API hook for delegating lifecycle events to Infinispan sub-modules.
 * <p>
 * For example, the 'tree' module needs to register specific types with the StreamingMarshaller. The 'query'
 * module needs to register an interceptor with the Cache if the Cache has enabled querying etc etc.
 * <p />
 * To use this hook, you would need to implement this interface (or extend {@link AbstractModuleLifecycle})
 * and then create a file called <tt>infinispan-module.properties</tt> in the root of your module's JAR.
 * When using Maven, for example, <tt>infinispan-module.properties</tt> would typically be in the module's
 * <tt>src/main/resources</tt> directory so it gets packaged appropriately.
 * <p />
 * <u>infinispan-modules.properties</u>
 * <p />
 * Currently, the following properties are supported:
 * <ul>
 * <li><tt>infinispan.module.name</tt> - the name of the module</li>
 * <li><tt>infinispan.module.lifecycle</tt> - the name of the class implementing {@link ModuleLifecycle}.  This implementation would typically reside in the module's codebase.</li>
 * </ul>
 *
 * @author Manik Surtani
 * @since 4.0
 */
public interface ModuleLifecycle {
    void cacheManagerStarting(GlobalComponentRegistry gcr);

    void cacheManagerStarted(GlobalComponentRegistry gcr);

    void cacheManagerStopping(GlobalComponentRegistry gcr);

    void cacheManagerStopped(GlobalComponentRegistry gcr);

    void cacheStarting(ComponentRegistry cr, String cacheName);

    void cacheStarted(ComponentRegistry cr, String cacheName);

    void cacheStopping(ComponentRegistry cr, String cacheName);

    void cacheStopped(ComponentRegistry cr, String cacheName);
}
