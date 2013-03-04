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
 * PARTICULAR PURPOSE. See the GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License,
 * v.2.1 along with this distribution; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA 02110-1301, USA.
 */

package org.infinispan.util.concurrent;

/**
 * A special Runnable (for the particular case of Total Order) that is only sent to a thread when it is ready to be
 * executed without blocking the thread
 * <p/>
 * Use case: - in Total Order, when the prepare is delivered, the runnable blocks waiting for the previous conflicting
 * transactions to be finished. In a normal executor service, this will take a thread and that thread will be blocked.
 * This way, the runnable waits on the queue and not in the Thread
 * <p/>
 * Used in {@code org.infinispan.util.concurrent.BlockingTaskAwareExecutorService}
 *
 * @author Pedro Ruivo
 * @since 5.3
 */
public interface BlockingRunnable extends Runnable {

   /**
    * @return true if this Runnable is ready to be executed without blocking
    */
   boolean isReady();

}
