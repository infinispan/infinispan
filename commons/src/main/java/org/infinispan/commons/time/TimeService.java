package org.infinispan.commons.time;

import java.time.Instant;
import java.util.concurrent.TimeUnit;

/**
 * Encapsulates all the time related logic in this interface.
 *
 * @author Pedro Ruivo
 * @since 5.3
 */
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
    * @return the current {@link Instant}. Similarly to {@link #time()}, note that some platforms do not provide nanosecond precision.
    */
   Instant instant();

   /**
    * It is equivalent to {@code timeDuration(startTime, time(), outputTimeUnit)}.
    *
    * @param startTimeNanos start cpu time in nanoseconds, usually returned by {@link #time()}.
    * @param outputTimeUnit the {@link TimeUnit} of the returned value.
    * @return the duration between the current cpu time and startTime. It returns zero if startTime is less than zero or
    *         if startTime is greater than the current cpu time.
    */
   long timeDuration(long startTimeNanos, TimeUnit outputTimeUnit);

   /**
    * @param startTimeNanos start cpu time in nanoseconds, usually returned by {@link #time()}.
    * @param endTimeNanos   end cpu time in nanoseconds, usually returned by {@link #time()}.
    * @param outputTimeUnit the {@link TimeUnit} of the returned value.
    * @return the duration between the endTime and startTime. It returns zero if startTime or endTime are less than zero
    *         or if startTime is greater than the endTime.
    */
   long timeDuration(long startTimeNanos, long endTimeNanos, TimeUnit outputTimeUnit);

   /**
    * @param endTimeNanos a cpu time in nanoseconds, usually returned by {@link #time()}
    * @return {@code true} if the endTime is less or equals than the current cpu time.
    */
   boolean isTimeExpired(long endTimeNanos);

   /**
    * @param endTimeNanos   the end cpu time in nanoseconds.
    * @param outputTimeUnit the {@link TimeUnit} of the returned value.
    * @return the remaining cpu time until the endTime is reached.
    */
   long remainingTime(long endTimeNanos, TimeUnit outputTimeUnit);

   /**
    * @param duration      the duration.
    * @param inputTimeUnit the {@link TimeUnit} of the duration.
    * @return the expected end time in nano seconds. If duration is less or equals to zero, the current cpu time is returned ({@link
    *         #time()}).
    */
   long expectedEndTime(long duration, TimeUnit inputTimeUnit);

}
