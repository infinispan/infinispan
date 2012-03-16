/*
 * JBoss, Home of Professional Open Source
 * Copyright 2010 Red Hat Inc. and/or its affiliates and other
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
package org.infinispan.query.test;

import org.hibernate.search.annotations.Field;
import org.hibernate.search.annotations.FieldBridge;
import org.hibernate.search.annotations.Indexed;
import org.hibernate.search.annotations.ProvidedId;
import org.hibernate.search.annotations.Store;
import org.hibernate.search.bridge.builtin.StringBridge;

import java.io.Serializable;

@ProvidedId(bridge = @FieldBridge(impl = StringBridge.class))
@Indexed(index = "anotherclass")
public class AnotherGrassEater implements Serializable {
   private static final long serialVersionUID = -5685487467005726138L;
   @Field(store = Store.YES)
   private String name;
   @Field(store = Store.YES)
   private String blurb;

   public AnotherGrassEater() {
   }

   public AnotherGrassEater(String name, String blurb) {
      this.name = name;
      this.blurb = blurb;
   }

   public String getName() {
      return name;
   }

   public void setName(String name) {
      this.name = name;
   }

   public String getBlurb() {
      return blurb;
   }

   public void setBlurb(String blurb) {
      this.blurb = blurb;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      AnotherGrassEater that = (AnotherGrassEater) o;

      if (blurb != null ? !blurb.equals(that.blurb) : that.blurb != null) return false;
      if (name != null ? !name.equals(that.name) : that.name != null) return false;

      return true;
   }

   @Override
   public int hashCode() {
      int result = name != null ? name.hashCode() : 0;
      result = 31 * result + (blurb != null ? blurb.hashCode() : 0);
      return result;
   }

   @Override
   public String toString() {
      return "AnotherGrassEater{" +
              "name='" + name + '\'' +
              ", blurb='" + blurb + '\'' +
              '}';
   }
}
