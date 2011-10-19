/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2011, Red Hat Middleware LLC, and individual contributors
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
package org.infinispan.query.clustered.commandworkers;

import java.io.IOException;

import org.hibernate.search.query.engine.spi.DocumentExtractor;
import org.infinispan.Cache;
import org.infinispan.query.backend.KeyTransformationHandler;
import org.infinispan.query.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * QueryExtractorUtil.
 * 
 * Utility to extract the cache key of a DocumentExtractor.
 * 
 * @author Israel Lacerra <israeldl@gmail.com>
 * @author Marko Luksa
 * @since 5.1
 */
public class QueryExtractorUtil {

   private static final Log log = LogFactory.getLog(QueryExtractorUtil.class, Log.class);

   private QueryExtractorUtil() {
   }

   public static Object extractKey(DocumentExtractor extractor, Cache cache, KeyTransformationHandler keyTransformationHandler, int docIndex) {
      String bufferDocumentId;
      try {
         bufferDocumentId = (String) extractor.extract(docIndex).getId();
      } catch (IOException e) {
         log.error("Error while extracting key...", e);
         return null;
      }

      Object key = keyTransformationHandler.stringToKey(bufferDocumentId, cache
            .getAdvancedCache().getClassLoader());
      return key;
   }

}
