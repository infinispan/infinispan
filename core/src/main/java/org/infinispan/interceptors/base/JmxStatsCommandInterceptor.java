/*
 * JBoss, Home of Professional Open Source
 * Copyright 2009 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
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
package org.infinispan.interceptors.base;

import org.infinispan.factories.annotations.Start;
import org.infinispan.jmx.JmxStatisticsExposer;
import org.infinispan.jmx.annotations.ManagedAttribute;
import org.infinispan.jmx.annotations.ManagedOperation;

/**
 * Base class for all the interceptors exposing management statistics.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
public abstract class JmxStatsCommandInterceptor extends CommandInterceptor implements JmxStatisticsExposer {
   @ManagedAttribute(description = "Enables or disables the gathering of statistics by this component", writable = true)
   private boolean statisticsEnabled = false;

   @Start
   public void checkStatisticsUsed() {
      setStatisticsEnabled(configuration.isExposeJmxStatistics());
   }

   /**
    * Returns whether an interceptor's statistics are being captured.
    *
    * @return true if statistics are captured
    */
   public boolean getStatisticsEnabled() {
      return statisticsEnabled;
   }

   /**
    * @param enabled whether gathering statistics for JMX are enabled.
    */
   public void setStatisticsEnabled(boolean enabled) {
      statisticsEnabled = enabled;
   }

   /**
    * Resets statistics gathered.  Is a no-op, and should be overridden if it is to be meaningful.
    */
   @ManagedOperation(description = "Resets statistics gathered by this component")
   public void resetStatistics() {
   }
}
