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
package org.infinispan.query.impl;

import org.infinispan.factories.ComponentRegistry;
import org.infinispan.lifecycle.AbstractModuleLifecycle;

public class LifecycleManager extends AbstractModuleLifecycle {

    @Override
    public void cacheStarted(ComponentRegistry cr, String cacheName) {
       // TODO: at this point, initialise the query interceptor and inject into the cache's interceptor chain?  Essentially the work done in QueryHelper
       // this can only be completed once we have HSEARCH-397 in place so that the indexable types can be gathered on the fly, rather than a-priori
    }

    @Override
    public void cacheStopped(ComponentRegistry cr, String cacheName) {
       // TODO: do we need to "shut down" anything in Hibernate Search?
    }
}
