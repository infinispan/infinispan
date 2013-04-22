/*
 * Copyright 2011 Red Hat, Inc. and/or its affiliates.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA
 */
package org.infinispan.configuration.cache;

import org.infinispan.configuration.AbstractTypedPropertiesConfiguration;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.util.TypedProperties;

/**
 * Describes a custom interceptor
 */
public class InterceptorConfiguration extends AbstractTypedPropertiesConfiguration {

   /**
    * Positional placing of a new custom interceptor
    */
   public static enum Position {
      /**
       * Specifies that the new interceptor is placed first in the chain.
       */
      FIRST,
      /**
       * Specifies that the new interceptor is placed last in the chain.
       */
      LAST,
      /**
       * Specifies that the new interceptor can be placed anywhere, except first or last.  This is the default, if not
       * explicitly specified.
       */
      OTHER_THAN_FIRST_OR_LAST
   }

   private final Class<? extends CommandInterceptor> after;
   private final Class<? extends CommandInterceptor> before;
   private final CommandInterceptor interceptor;
   private final int index;
   private final Position position;

   InterceptorConfiguration(Class<? extends CommandInterceptor> after, Class<? extends CommandInterceptor> before,
         CommandInterceptor interceptor, int index, Position position, TypedProperties properties) {
      super(properties);
      this.after = after;
      this.before = before;
      this.interceptor = interceptor;
      this.index = index;
      this.position = position;
   }

   public Class<? extends CommandInterceptor> after() {
      return after;
   }

   public Class<? extends CommandInterceptor> before() {
      return before;
   }

   public CommandInterceptor interceptor() {
      return interceptor;
   }

   public int index() {
      return index;
   }

   public Position position() {
      return position;
   }

   public boolean first() {
      return position() == Position.FIRST;
   }

   public boolean last() {
      return position() == Position.LAST;
   }

   @Override
   public String toString() {
      return "InterceptorConfiguration [after=" + after + ", before=" + before + ", interceptor=" + interceptor + ", index=" + index + ", position=" + position + ", properties="
            + properties() + "]";
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      InterceptorConfiguration that = (InterceptorConfiguration) o;

      if (index != that.index) return false;
      if (after != null ? !after.equals(that.after) : that.after != null)
         return false;
      if (before != null ? !before.equals(that.before) : that.before != null)
         return false;
      if (interceptor != null ? !interceptor.equals(that.interceptor) : that.interceptor != null)
         return false;
      if (position != that.position) return false;

      return true;
   }

   @Override
   public int hashCode() {
      int result = after != null ? after.hashCode() : 0;
      result = 31 * result + (before != null ? before.hashCode() : 0);
      result = 31 * result + (interceptor != null ? interceptor.hashCode() : 0);
      result = 31 * result + index;
      result = 31 * result + (position != null ? position.hashCode() : 0);
      return result;
   }

}
