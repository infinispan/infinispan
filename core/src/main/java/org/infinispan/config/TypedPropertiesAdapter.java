/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2009, Red Hat Middleware LLC, and individual contributors
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
package org.infinispan.config;

import java.util.Set;
import java.util.Map.Entry;

import javax.xml.bind.annotation.adapters.XmlAdapter;
import org.infinispan.util.TypedProperties;

public class TypedPropertiesAdapter extends XmlAdapter<PropertiesType, TypedProperties> {

   @Override
   public PropertiesType marshal(TypedProperties tp) throws Exception {
      PropertiesType pxml = new PropertiesType();
      Property[] pa = new Property[tp.size()];
      Set<Entry<Object, Object>> set = tp.entrySet();
      int index = 0;
      for (Entry<Object, Object> entry : set) {
         pa[index] = new Property();
         pa[index].name = (String) entry.getKey();
         pa[index].value = (String) entry.getValue();
         index++;
      }
      pxml.properties = pa;
      return pxml;
   }

   @Override
   public TypedProperties unmarshal(PropertiesType props) throws Exception {
      TypedProperties tp = new TypedProperties();
      for (Property p : props.properties) {
         tp.put(p.name, p.value);
      }
      return tp;
   }
}
