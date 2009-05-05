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
package org.infinispan.util.concurrent;

import org.infinispan.CacheException;


/**
 * Thrown when a timeout occurred. used by operations with timeouts, e.g. lock acquisition, or waiting for responses
 * from all members.
 *
 * @author <a href="mailto:bela@jboss.org">Bela Ban</a>.
 *         <p/>
 *         <p/>
 *         <p><b>Revisions:</b>
 *         <p/>
 *         <p>Dec 28 2002 Bela Ban: first implementation
 * @since 4.0
 */
public class TimeoutException extends CacheException {

   /**
    * The serialVersionUID
    */
   private static final long serialVersionUID = -8096787619908687038L;

   public TimeoutException() {
      super();
   }

   public TimeoutException(String msg) {
      super(msg);
   }

   public TimeoutException(String msg, Throwable cause) {
      super(msg, cause);
   }

   @Override
   public String toString() {
      return super.toString();
   }
}
