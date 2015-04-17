package org.infinispan.query.indexmanager;

import java.io.Serializable;

import org.hibernate.search.backend.AddLuceneWork;
import org.hibernate.search.backend.DeleteLuceneWork;
import org.hibernate.search.backend.FlushLuceneWork;
import org.hibernate.search.backend.IndexWorkVisitor;
import org.hibernate.search.backend.LuceneWork;
import org.hibernate.search.backend.OptimizeLuceneWork;
import org.hibernate.search.backend.PurgeAllLuceneWork;
import org.hibernate.search.backend.UpdateLuceneWork;
import org.hibernate.search.backend.spi.DeleteByQueryLuceneWork;
import org.infinispan.query.backend.KeyTransformationHandler;

/**
 * The serialized form of LuceneWork needs to be adjusted after deserialization to apply
 * our custom keyTransformers. LuceneWork instances are immutable, so we have to replace them
 * with new instances iff an id transformation is needed.
 *
 * @author Sanne Grinovero <sanne@hibernate.org> (C) 2012 Red Hat Inc.
 */
public class LuceneWorkTransformationVisitor implements IndexWorkVisitor<KeyTransformationHandler, LuceneWork> {

   static final LuceneWorkTransformationVisitor INSTANCE = new LuceneWorkTransformationVisitor();

   private static final LuceneWorkIdTransformer<AddLuceneWork> addDuplicator = new AddWorkDuplicator();
   private static final LuceneWorkIdTransformer<DeleteLuceneWork> deleteDuplicator = new DeleteWorkDuplicator();
   private static final LuceneWorkIdTransformer<UpdateLuceneWork> updateDuplicator = new UpdateWorkDuplicator();
   private static final LuceneWorkIdTransformer<LuceneWork> returnSameDuplicator = new NotReallyDuplicator();

   private LuceneWorkTransformationVisitor() {
      //no instances needed
   }

   @Override
   public LuceneWork visitAddWork(AddLuceneWork addLuceneWork, KeyTransformationHandler keyTransformationHandler) {
      return addDuplicator.cloneOverridingIdString( addLuceneWork, keyTransformationHandler );
   }

   @Override
   public LuceneWork visitDeleteWork(DeleteLuceneWork deleteLuceneWork, KeyTransformationHandler keyTransformationHandler) {
      return deleteDuplicator.cloneOverridingIdString( deleteLuceneWork, keyTransformationHandler );
   }

   @Override
   public LuceneWork visitUpdateWork(UpdateLuceneWork updateLuceneWork, KeyTransformationHandler keyTransformationHandler) {
      return updateDuplicator.cloneOverridingIdString( updateLuceneWork, keyTransformationHandler );
   }

   @Override
   public LuceneWork visitOptimizeWork(OptimizeLuceneWork optimizeLuceneWork, KeyTransformationHandler keyTransformationHandler) {
      return returnSameDuplicator.cloneOverridingIdString( optimizeLuceneWork, keyTransformationHandler );
   }

   @Override
   public LuceneWork visitFlushWork(FlushLuceneWork flushLuceneWork, KeyTransformationHandler keyTransformationHandler) {
      return returnSameDuplicator.cloneOverridingIdString( flushLuceneWork, keyTransformationHandler );
   }

   @Override
   public LuceneWork visitPurgeAllWork(PurgeAllLuceneWork purgeAllLuceneWork, KeyTransformationHandler keyTransformationHandler) {
      return returnSameDuplicator.cloneOverridingIdString( purgeAllLuceneWork, keyTransformationHandler );
   }

   @Override
   public LuceneWork visitDeleteByQueryWork(DeleteByQueryLuceneWork work, KeyTransformationHandler p) {
	   throw new UnsupportedOperationException( "delete-by-query is not supported" );
   }

   private static class AddWorkDuplicator implements LuceneWorkIdTransformer<AddLuceneWork> {
      @Override
      public AddLuceneWork cloneOverridingIdString(final AddLuceneWork lw, final KeyTransformationHandler keyTransformationHandler) {
         final Serializable id = lw.getId();
         if (id == null) {
            //this is serialized work received from a remote node: take the getIdAsString instead
            final String idInString = lw.getIdInString();
            return new AddLuceneWork(idInString, idInString, lw.getEntityClass(), lw.getDocument(), lw.getFieldToAnalyzerMap());
         }
         else {
            return lw;
         }
      }
   }

   private static class UpdateWorkDuplicator implements LuceneWorkIdTransformer<UpdateLuceneWork> {
      @Override
      public UpdateLuceneWork cloneOverridingIdString(final UpdateLuceneWork lw, final KeyTransformationHandler keyTransformationHandler) {
         final Serializable id = lw.getId();
         if (id == null) {
            //this is serialized work received from a remote node: take the getIdAsString instead
            final String idInString = lw.getIdInString();
            return new UpdateLuceneWork(idInString, idInString, lw.getEntityClass(), lw.getDocument(), lw.getFieldToAnalyzerMap());
         }
         else {
            return lw;
         }
      }
   }

   private static class DeleteWorkDuplicator implements LuceneWorkIdTransformer<DeleteLuceneWork> {
      @Override
      public DeleteLuceneWork cloneOverridingIdString(final DeleteLuceneWork lw, final KeyTransformationHandler keyTransformationHandler) {
         final Serializable id = lw.getId();
         if (id == null) {
            //this is serialized work received from a remote node: take the getIdAsString instead
            final String idInString = lw.getIdInString();
            return new DeleteLuceneWork(idInString, idInString, lw.getEntityClass());
         }
         else {
            return lw;
         }
      }
   }

   /**
    * The remaining cases don't need any key transformation, so we return the same instance.
    * Not particularly tricky since these are immutable anyway.
    */
   private static class NotReallyDuplicator implements LuceneWorkIdTransformer<LuceneWork> {
      @Override
      public LuceneWork cloneOverridingIdString(final LuceneWork lw, final KeyTransformationHandler keyTransformationHandler) {
         return lw;
      }
   }
}
