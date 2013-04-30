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
package org.infinispan.query.backend;

import org.hibernate.search.backend.spi.Work;
import org.hibernate.search.backend.spi.WorkType;

import java.io.Serializable;
import java.util.Collection;

/**
 * Creates collections of Work instances that should be performed by Hibernate-Search.
 *
 * @author Marko Luksa
 */
public interface SearchWorkCreator<T> {

   /**
    * Creates a collection of Work instances that Hibernate-Search should perform for all the entities of the given
    * entity type.
    * @param entityType the entity type that these Works should be created for
    * @param workType the type of work to be done
    * @return collection of Work instances
    */
   Collection<Work<T>> createPerEntityTypeWorks(Class<T> entityType, WorkType workType);

   /**
    * Creates a collection of Work instances that Hibernate-Search should perform for the given entity
    * @param entity the entity that these Works should be created for
    * @param id the id of the document
    * @param workType the type of work to be done
    * @return collection of Work instances
    */
   Collection<Work<T>> createPerEntityWorks(T entity, Serializable id, WorkType workType);
}
