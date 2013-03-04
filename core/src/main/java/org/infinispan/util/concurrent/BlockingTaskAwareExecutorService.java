/*
 * JBoss, Home of Professional Open Source
 * Copyright 2013 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License, v. 2.1.
 * This program is distributed in the hope that it will be useful, but WITHOUT A
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License,
 * v.2.1 along with this distribution; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA  02110-1301, USA.
 */

package org.infinispan.util.concurrent;

import java.util.concurrent.ExecutorService;

/**
 * Executor service that is aware of {@code BlockingRunnable} and only dispatch the runnable to a thread when it has low
 * (or no) probability of blocking the thread.
 * <p/>
 * However, it is not aware of the changes in the state so you must invoke {@link #checkForReadyTasks()} to notify
 * this that some runnable may be ready to be processed.
 *
 * @author Pedro Ruivo
 * @since 5.3
 */
public interface BlockingTaskAwareExecutorService extends ExecutorService {

   /**
    * Executes the given command at some time in the future when the command is less probably to block a thread.
    *
    * @param runnable the command to execute
    */
   void execute(BlockingRunnable runnable);

   /**
    * It checks for tasks ready to be processed in the thread.
    */
   void checkForReadyTasks();

}
