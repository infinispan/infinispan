/*
 * JBoss, Home of Professional Open Source
 * Copyright 2013 Red Hat Inc. and/or its affiliates and other
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
package org.infinispan.query.queries;

import org.hibernate.search.annotations.Analyze;
import org.hibernate.search.annotations.Field;
import org.hibernate.search.annotations.Indexed;
import org.hibernate.search.annotations.Store;

/**
 * A new additional entity type for testing Infinispan Querying.
 *
 * @author Anna Manukyan
 */
@Indexed(index = "person")
public class NumericType {
   @Field(store = Store.YES, analyze = Analyze.YES)
   private int num1;
   @Field(store = Store.YES, analyze = Analyze.YES)
   private int num2;

   @Field(store = Store.YES, analyze = Analyze.NO)
   private String name;

   public NumericType(int num1, int num2) {
      this.num1 = num1;
      this.num2 = num2;
   }

   public void setName(String name) {
      this.name = name;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      NumericType that = (NumericType) o;

      if (num1 != that.num1) return false;
      if (num2 != that.num2) return false;

      return true;
   }

   @Override
   public int hashCode() {
      int result = num1;
      result = 31 * result + num2;
      return result;
   }
}
