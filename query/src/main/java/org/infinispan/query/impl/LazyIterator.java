/*
 * JBoss, Home of Professional Open Source
 * Copyright 2009 Red Hat Inc. and/or its affiliates and other
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

import java.io.IOException;

import net.jcip.annotations.NotThreadSafe;
import org.hibernate.search.query.engine.spi.DocumentExtractor;
import org.hibernate.search.query.engine.spi.EntityInfo;
import org.hibernate.search.query.engine.spi.HSQuery;
import org.infinispan.CacheException;

/**
 * Implementation for {@link org.infinispan.query.ResultIterator}. This loads the results only when required
 * and hence differs from {@link EagerIterator} which is the other implementation of ResultIterator.
 *
 * @author Navin Surtani
 * @author Marko Luksa
 * @author Ales Justin
 */
@NotThreadSafe
public class LazyIterator extends AbstractIterator {

   private final DocumentExtractor extractor;

   public LazyIterator(HSQuery hSearchQuery, QueryResultLoader resultLoader, int fetchSize) {
      super(resultLoader, fetchSize);
      this.extractor = hSearchQuery.queryDocumentExtractor(); //triggers actual Lucene search
      this.index = extractor.getFirstIndex();
      this.max = hSearchQuery.queryResultSize() - 1;
   }

   @Override
   public void close() {
      extractor.close();
   }

   protected EntityInfo loadEntityInfo(int index) {
      try {
         return extractor.extract(index);
      } catch (IOException e) {
         throw new CacheException("Cannot load result at index " + index, e);
      }
   }
}
