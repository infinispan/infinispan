/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2009, Red Hat, Inc. and/or its affiliates, and
 * individual contributors as indicated by the @author tags. See the
 * copyright.txt file in the distribution for a full listing of
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
package org.infinispan.server.memcached;

import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

/**
 * DelayedDeleteEntry.
 * 
 * @author Galder Zamarreño
 * @since 4.0
 */
public class DeleteDelayedEntry implements Delayed {

   final String key;
   private final long time;
   private final boolean isUnix;

   DeleteDelayedEntry(String key, long time) {
      this.time = time;
      this.key = key;
      this.isUnix = time > TextProtocolUtil.SECONDS_IN_A_MONTH;
   }

   @Override
   public long getDelay(TimeUnit unit) {
      long now = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());
      return unit.convert(time - now, TimeUnit.SECONDS);
   }

   @Override
   public int compareTo(Delayed o) {
      if (o == this)
         return 0;

      if (o instanceof DeleteDelayedEntry) {
         DeleteDelayedEntry x = (DeleteDelayedEntry) o;
         long diff = time - x.time;
         if (diff < 0) return -1;
         else if (diff > 0) return 1;
         else return 0;
      } else {
         throw new ClassCastException(o.getClass() + " is not of type " + DeleteDelayedEntry.class);
      }
   }

}
