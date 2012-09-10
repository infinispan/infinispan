/*
 * JBoss, Home of Professional Open Source
 * Copyright 2011 Red Hat Inc. and/or its affiliates and other
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

package org.infinispan.query.queries.faceting;

import java.io.Serializable;

import org.hibernate.search.annotations.Analyze;
import org.hibernate.search.annotations.Field;
import org.hibernate.search.annotations.Indexed;
import org.hibernate.search.annotations.Store;

/**
 * @author Hardy Ferentschik
 */
@Indexed
public class Car implements Serializable {

   @Field(analyze=Analyze.NO)
   private String color;

   @Field(store = Store.YES)
   private String make;

   @Field(analyze=Analyze.NO)
   private int cubicCapacity;

   public Car(String make, String color, int cubicCapacity) {
      this.color = color;
      this.cubicCapacity = cubicCapacity;
      this.make = make;
   }

   public String getColor() {
      return color;
   }

   public int getCubicCapacity() {
      return cubicCapacity;
   }

   public String getMake() {
      return make;
   }

   @Override
   public String toString() {
      final StringBuilder sb = new StringBuilder();
      sb.append("Car");
      sb.append("{color='").append(color).append('\'');
      sb.append(", make='").append(make).append('\'');
      sb.append(", cubicCapacity=").append(cubicCapacity);
      sb.append('}');
      return sb.toString();
   }
}
