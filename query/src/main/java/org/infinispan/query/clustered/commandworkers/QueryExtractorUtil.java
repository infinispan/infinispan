package org.infinispan.query.clustered.commandworkers;

import java.io.IOException;

import org.hibernate.search.exception.SearchException;
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

   public static Object extractKey(DocumentExtractor extractor, Cache<?, ?> cache, KeyTransformationHandler keyTransformationHandler, int docIndex) {
      String bufferDocumentId;
      try {
         bufferDocumentId = (String) extractor.extract(docIndex).getId();
      } catch (IOException e) {
         log.error("Error while extracting key...", e);
         return null;
      }

      return keyTransformationHandler.stringToKey(bufferDocumentId, cache
            .getAdvancedCache().getClassLoader());
   }

   static Object[] extractProjection(DocumentExtractor extractor, int docIndex) {
      try {
         return extractor.extract(docIndex).getProjection();
      } catch (IOException e) {
         throw new SearchException("Error while extracting projection...", e);
      }
   }

}
