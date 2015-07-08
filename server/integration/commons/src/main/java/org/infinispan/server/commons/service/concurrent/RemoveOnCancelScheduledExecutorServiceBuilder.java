/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2013, Red Hat, Inc., and individual contributors
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
package org.infinispan.server.commons.service.concurrent;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;

import org.infinispan.server.commons.service.AsynchronousServiceBuilder;
import org.infinispan.server.commons.service.Builder;
import org.jboss.msc.service.Service;
import org.jboss.msc.service.ServiceBuilder;
import org.jboss.msc.service.ServiceController;
import org.jboss.msc.service.ServiceName;
import org.jboss.msc.service.ServiceTarget;
import org.jboss.msc.service.StartContext;
import org.jboss.msc.service.StopContext;
import org.jboss.threads.JBossExecutors;

/**
 * Service that provides a {@link ScheduledThreadPoolExecutor} that removes tasks from the task queue upon cancellation.
 * @author Paul Ferraro
 */
public class RemoveOnCancelScheduledExecutorServiceBuilder implements Builder<ScheduledExecutorService>, Service<ScheduledExecutorService> {

    private final ServiceName name;
    private final ThreadFactory factory;
    private volatile int size = 1;

    private volatile ScheduledExecutorService executor;

    public RemoveOnCancelScheduledExecutorServiceBuilder(ServiceName name, ThreadFactory factory) {
        this.name = name;
        this.factory = factory;
    }

    @Override
    public ServiceName getServiceName() {
        return this.name;
    }

    @Override
    public ServiceBuilder<ScheduledExecutorService> build(ServiceTarget target) {
        return new AsynchronousServiceBuilder<>(this.name, this).startSynchronously().build(target).setInitialMode(ServiceController.Mode.ON_DEMAND);
    }

    public RemoveOnCancelScheduledExecutorServiceBuilder size(int size) {
        this.size = size;
        return this;
    }

    @Override
    public ScheduledExecutorService getValue() {
        return JBossExecutors.protectedScheduledExecutorService(this.executor);
    }

    @Override
    public void start(StartContext context) {
        ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(this.size, this.factory);
        executor.setRemoveOnCancelPolicy(true);
        executor.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
        this.executor = executor;
    }

    @Override
    public void stop(StopContext context) {
        this.executor.shutdown();
        this.executor = null;
    }
}
