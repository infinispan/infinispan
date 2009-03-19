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
package org.horizon.notifications.cachelistener.event;

/**
 * This event is passed in to any method annotated with {@link org.horizon.notifications.cachelistener.annotation.TransactionCompleted}.
 * <p/>
 * Note that this event is only delivered <i>after the fact</i>, i.e., you will never see an instance of this event with
 * {@link #isPre()} being set to <tt>true</tt>.
 *
 * @author <a href="mailto:manik@jboss.org">Manik Surtani</a>
 * @since 1.0
 */
public interface TransactionCompletedEvent extends TransactionalEvent {
   /**
    * @return if <tt>true</tt>, the transaction completed by committing successfully.  If <tt>false</tt>, the
    *         transaction completed with a rollback.
    */
   boolean isTransactionSuccessful();
}
