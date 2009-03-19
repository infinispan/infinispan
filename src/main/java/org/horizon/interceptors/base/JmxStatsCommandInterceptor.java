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
package org.horizon.interceptors.base;

import org.horizon.factories.annotations.Start;
import org.horizon.jmx.JmxStatisticsExposer;
import org.horizon.jmx.annotations.ManagedAttribute;
import org.horizon.jmx.annotations.ManagedOperation;

/**
 * Base class for all the interceptors exposing management statistics.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
public abstract class JmxStatsCommandInterceptor extends CommandInterceptor implements JmxStatisticsExposer {
   private boolean statsEnabled = false;

   @Start
   public void checkStatisticsUsed() {
      setStatisticsEnabled(configuration.isExposeManagementStatistics());
   }

   /**
    * Returns whether an interceptor's statistics are being captured.
    *
    * @return true if statistics are captured
    */
   @ManagedAttribute
   public boolean getStatisticsEnabled() {
      return statsEnabled;
   }

   /**
    * @param enabled whether gathering statistics for JMX are enabled.
    */
   @ManagedAttribute
   public void setStatisticsEnabled(boolean enabled) {
      statsEnabled = enabled;
   }

   /**
    * Resets statistics gathered.  Is a no-op, and should be overridden if it is to be meaningful.
    */
   @ManagedOperation
   public void resetStatistics() {
   }
}
