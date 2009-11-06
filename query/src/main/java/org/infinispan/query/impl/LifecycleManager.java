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
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.factories.ModuleLifecycle;

public class LifecycleManager implements ModuleLifecycle {

    @Override
    public void cacheManagerStarted(GlobalComponentRegistry gcr) {
        System.out.println("cacheManagerStarted");
    }

    @Override
    public void cacheManagerStarting(GlobalComponentRegistry gcr) {
        System.out.println("cacheManagerStarting");
    }

    @Override
    public void cacheManagerStopped(GlobalComponentRegistry gcr) {
        System.out.println("cacheManagerStopped");
    }

    @Override
    public void cacheManagerStopping(GlobalComponentRegistry gcr) {
        System.out.println("cacheManagerStopping");
    }

    @Override
    public void cacheStarted(ComponentRegistry cr, String cacheName) {
        System.out.println("cacheStarted");
    }

    @Override
    public void cacheStarting(ComponentRegistry cr, String cacheName) {
        System.out.println("cacheStarting");
    }

    @Override
    public void cacheStopped(ComponentRegistry cr, String cacheName) {
        System.out.println("cacheStopped " + cacheName);
    }

    @Override
    public void cacheStopping(ComponentRegistry cr, String cacheName) {
        System.out.println("cacheStopping " + cacheName);
    }
}
