/*
 * JBoss, Home of Professional Open Source
 * Copyright 2013 Red Hat Inc. and/or its affiliates and other contributors
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
package org.infinispan.query.impl;

import org.hibernate.search.backend.spi.Work;
import org.hibernate.search.backend.spi.WorkType;
import org.infinispan.query.backend.SearchWorkCreator;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;

/**
 * @author Marko Luksa
 */
public class DefaultSearchWorkCreator<T> implements SearchWorkCreator<T> {

   @Override
   public Collection<Work<T>> createPerEntityTypeWorks(Class<T> entityType, WorkType workType) {
      Work<T> work = new Work<T>(entityType, null, workType);
      return Collections.singleton(work);
   }

   @Override
   public Collection<Work<T>> createPerEntityWorks(T entity, Serializable id, WorkType workType) {
      Work<T> work = new Work<T>(entity, id, workType);
      return Collections.singleton(work);
   }
}
