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
package org.infinispan.loaders.modifications;

import java.util.List;

/**
 * ModificationsList contains a List<Modification>
 * 
 * @author Sanne Grinovero
 * @since 4.1
 */
public class ModificationsList implements Modification {
   
   private final List<? extends Modification> list;

   public ModificationsList(List<? extends Modification> list) {
      this.list = list;
   }

   @Override
   public Type getType() {
      return Modification.Type.LIST;
   }

   public List<? extends Modification> getList() {
      return list;
   }

   @Override
   public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((list == null) ? 0 : list.hashCode());
      return result;
   }

   @Override
   public boolean equals(Object obj) {
      if (this == obj)
         return true;
      if (obj == null)
         return false;
      if (getClass() != obj.getClass())
         return false;
      ModificationsList other = (ModificationsList) obj;
      if (list == null) {
         if (other.list != null)
            return false;
      } else if (!list.equals(other.list))
         return false;
      return true;
   }
   
   @Override
   public String toString() {
      return "ModificationsList: [" + list + "]";
   }

}
