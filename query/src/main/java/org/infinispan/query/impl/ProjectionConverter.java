/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other
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
package org.infinispan.query.impl;

import org.infinispan.AdvancedCache;
import org.infinispan.query.ProjectionConstants;
import org.infinispan.query.backend.KeyTransformationHandler;

import java.util.LinkedList;
import java.util.List;

/**
 * Converts between Infinispan and HSearch projection fields.
 *
 * @author <a href="mailto:mluksa@redhat.com">Marko Luksa</a>
 */
public class ProjectionConverter {

   private final AdvancedCache<?, ?> cache;
   private final KeyTransformationHandler keyTransformationHandler;
   private final String[] hibernateSearchFields;
   private final List<Integer> indexesOfKey = new LinkedList<Integer>();

   public ProjectionConverter(String[] fields, AdvancedCache<?, ?> cache, KeyTransformationHandler keyTransformationHandler) {
      this.cache = cache;
      this.keyTransformationHandler = keyTransformationHandler;

      hibernateSearchFields = fields.clone();
      for (int i = 0; i < hibernateSearchFields.length; i++) {
         String field = hibernateSearchFields[i];
         if (field.equals( ProjectionConstants.KEY )) {
            hibernateSearchFields[i] = ProjectionConstants.ID;
            indexesOfKey.add(i);
         }
      }
   }

   public String[] getHSearchProjection() {
      return hibernateSearchFields;
   }

   public Object[] convert(Object[] projection) {
      for (Integer index : indexesOfKey) {
         projection[index] = keyTransformationHandler.stringToKey((String) projection[index], cache.getClassLoader());
      }
      return projection;
   }
}
