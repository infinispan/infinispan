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

package org.infinispan.util;

import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;

import java.util.concurrent.TimeUnit;

/**
 * Encapsulates all the time related logic in this interface.
 *
 * @author Pedro Ruivo
 * @since 5.3
 */
@Scope(Scopes.GLOBAL)
public interface TimeService {

   /**
    * @return the current clock time in milliseconds. Note that it depends of the system time.
    */
   long wallClockTime();

   /**
    * @return the current cpu time in nanoseconds. Note that some platforms do not provide nanosecond precision.
    */
   long time();

   /**
    * It is equivalent to {@code timeDuration(startTime, time(), outputTimeUnit)}.
    *
    * @param startTime      start cpu time in nanoseconds, usually returned by {@link #time()}.
    * @param outputTimeUnit the {@link TimeUnit} of the returned value.
    * @return the duration between the current cpu time and startTime. It returns zero if startTime is less than zero or
    *         if startTime is greater than the current cpu time.
    */
   long timeDuration(long startTime, TimeUnit outputTimeUnit);

   /**
    * @param startTime      start cpu time in nanoseconds, usually returned by {@link #time()}.
    * @param endTime        end cpu time in nanoseconds, usually returned by {@link #time()}.
    * @param outputTimeUnit the {@link TimeUnit} of the returned value.
    * @return the duration between the endTime and startTime. It returns zero if startTime or endTime are less than zero
    *         or if startTime is greater than the endTime.
    */
   long timeDuration(long startTime, long endTime, TimeUnit outputTimeUnit);

   /**
    * @param endTime a cpu time in nanoseconds, usually returned by {@link #time()}
    * @return {@code true} if the endTime is less or equals than the current cpu time.
    */
   boolean isTimeExpired(long endTime);

   /**
    * @param endTime        the end cpu time in nanoseconds.
    * @param outputTimeUnit the {@link TimeUnit} of the returned value.
    * @return the remaining cpu time until the endTime is reached.
    */
   long remainingTime(long endTime, TimeUnit outputTimeUnit);

   /**
    * @param duration      the duration.
    * @param inputTimeUnit the {@link TimeUnit} of the duration.
    * @return the expected end time. If duration is less or equals to zero, the current cpu time is returned ({@link
    *         #time()}).
    */
   long expectedEndTime(long duration, TimeUnit inputTimeUnit);

}
