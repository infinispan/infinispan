/* 
 * JBoss, Home of Professional Open Source
 * Copyright 2011 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @authors tag. All rights reserved.
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

package org.infinispan.query.queries.faceting;

import org.hibernate.search.annotations.Field;
import org.hibernate.search.annotations.Index;
import org.hibernate.search.annotations.Indexed;
import org.hibernate.search.annotations.ProvidedId;
import org.hibernate.search.annotations.Store;

/**
 * @author Hardy Ferentschik
 */
@Indexed
@ProvidedId
public class Car {

   @Field(index = Index.UN_TOKENIZED)
   private String color;

   @Field(store = Store.YES)
   private String make;

   @Field(index = Index.UN_TOKENIZED)
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
