/**
 * Copyright (C) 2006-2009 Dustin Sallings Copyright (C) 2009-2013 Couchbase, Inc.
 * <p>
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
 * documentation files (the "Software"), to deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to the following conditions:
 * <p>
 * The above copyright notice and this permission notice shall be included in all copies or substantial portions of the
 * Software.
 * <p>
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE
 * WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
 * COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
 * OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALING IN THE SOFTWARE.
 */

package org.infinispan.server.memcached.test;

import java.text.SimpleDateFormat;
import java.util.Date;

import net.spy.memcached.compat.log.AbstractLogger;
import net.spy.memcached.compat.log.Level;

/**
 * Default logger implementation.
 *
 * This logger is really primitive. It just logs everything to stderr if it's
 * higher than INFO.
 */
public class DefaultLogger extends AbstractLogger {

   private final SimpleDateFormat df;

   /**
    * Get an instance of DefaultLogger.
    */
   public DefaultLogger(String name) {
      super(name);
      df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
   }

   /**
    * Always returns false, trace is not enabled on the DefaultLogger.
    */
   @Override
   public boolean isTraceEnabled() {
      return true;
   }

   /**
    * Always returns false, debug is not enabled on the DefaultLogger.
    */
   @Override
   public boolean isDebugEnabled() {
      return true;
   }

   /**
    * Always returns true, info is always enabled on the DefaultLogger.
    */
   @Override
   public boolean isInfoEnabled() {
      return true;
   }

   /**
    * @see AbstractLogger
    */
   @Override
   public synchronized void log(Level level, Object message, Throwable e) {
      System.err.printf("%s %s %s:  %s\n", df.format(new Date()), level.name(),
            getName(), message);
      if (e != null) {
         e.printStackTrace();
      }

   }
}
