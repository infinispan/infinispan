/*
 * JBoss, Home of Professional Open Source
 * Copyright 2011 Red Hat Inc. and/or its affiliates and other contributors
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

package org.infinispan.cacheviews;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import net.jcip.annotations.Immutable;

import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.marshall.Ids;
import org.infinispan.commons.util.Util;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.Immutables;

/**
 * A virtual view for a cache.
 */
@Immutable
public class CacheView {
   public static final CacheView EMPTY_CACHE_VIEW = new CacheView(-1, Collections.<Address>emptyList());

   private final int viewId;
   private final List<Address> members;

   public CacheView(int viewId, List<Address> members) {
      if (members == null)
         throw new IllegalArgumentException("Member list cannot be null");
      this.viewId = viewId;
      this.members = Immutables.immutableListCopy(members);
   }

   public int getViewId() {
      return viewId;
   }

   public List<Address> getMembers() {
      return members;
   }

   public boolean isEmpty() {
      return members.isEmpty();
   }

   public boolean contains(Address node) {
      return members.contains(node);
   }

   public boolean containsAny(Collection<Address> nodes) {
      for (Address node : nodes) {
         if (members.contains(node))
            return true;
      }

      return false;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      CacheView cacheView = (CacheView) o;

      if (viewId != cacheView.viewId) return false;
      if (members != null ? !members.equals(cacheView.members) : cacheView.members != null) return false;

      return true;
   }

   @Override
   public int hashCode() {
      int result = viewId;
      result = 31 * result + (members != null ? members.hashCode() : 0);
      return result;
   }

   @Override
   public String toString() {
      return "CacheView{" +
            "viewId=" + viewId +
            ", members=" + members +
            '}';
   }


   public static class Externalizer extends AbstractExternalizer<CacheView> {
      @Override
      public void writeObject(ObjectOutput output, CacheView cacheView) throws IOException {
         output.writeInt(cacheView.viewId);
         output.writeObject(cacheView.members);
      }

      @Override
      public CacheView readObject(ObjectInput unmarshaller) throws IOException, ClassNotFoundException {
         int viewId = unmarshaller.readInt();
         List<Address> members = (List<Address>) unmarshaller.readObject();
         return new CacheView(viewId, members);
      }

      @Override
      public Integer getId() {
         return Ids.CACHE_VIEW;
      }

      @Override
      public Set<Class<? extends CacheView>> getTypeClasses() {
         return Util.<Class<? extends CacheView>>asSet(CacheView.class);
      }
   }
}
