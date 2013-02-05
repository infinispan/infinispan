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

package org.infinispan.util.logging.log4j;

import java.util.regex.Pattern;

import org.apache.log4j.Level;
import org.apache.log4j.spi.Filter;
import org.apache.log4j.spi.LoggingEvent;

/**
 * Log4j {@link Filter} that only allow events from threads matching a regular expression.
 * Events with a level greater than {@code threshold} are always logged.
 *
 * @author Dan Berindei
 * @since 5.2
 */
public class ThreadNameFilter extends Filter {
   private Level threshold = Level.DEBUG;
   private Pattern includePattern;

   public Level getThreshold() {
      return threshold;
   }

   public void setThreshold(Level threshold) {
      this.threshold = threshold;
   }

   public String getInclude() {
      return includePattern != null ? includePattern.pattern() : null;
   }

   public void setInclude(String include) {
      this.includePattern = Pattern.compile(include);
   }

   @Override
   public int decide(LoggingEvent event) {
      if (event.getLevel().isGreaterOrEqual(threshold)) {
         return Filter.NEUTRAL;
      } else if (includePattern == null || includePattern.matcher(event.getThreadName()).find()) {
         return Filter.NEUTRAL;
      } else {
         return Filter.DENY;
      }
   }
}
