/*
 * JBoss, Home of Professional Open Source
 * Copyright ${year}, Red Hat Middleware LLC, and individual contributors
 * by the @authors tag. See the copyright.txt in the distribution for a
 * full listing of individual contributors.
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

package org.infinispan.query.backend;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Searcher;
import org.hibernate.search.reader.ReaderProvider;
import static org.hibernate.search.reader.ReaderProviderHelper.getIndexReaders;

import java.util.Set;

/**
 * Class with static method that is called by {@link org.infinispan.query.impl.CacheQueryImpl} and {@link
 * org.infinispan.query.impl.EagerIterator}
 * <p/>
 * <p/>
 * Simply an abstraction for one method to reduce unneccesary code replication.
 *
 * @author Navin Surtani
 */
public class IndexSearcherCloser {
   public static void closeSearcher(Searcher searcher, ReaderProvider readerProvider) {
      Set<IndexReader> indexReaders = getIndexReaders(searcher);

      for (IndexReader indexReader : indexReaders) {
         readerProvider.closeReader(indexReader);
      }
   }
}
