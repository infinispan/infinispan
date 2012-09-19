/* 
 * JBoss, Home of Professional Open Source 
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @author tag. All rights reserved. 
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
package org.infinispan.query.indexmanager;

import java.io.Serializable;

import org.hibernate.search.backend.AddLuceneWork;
import org.hibernate.search.backend.DeleteLuceneWork;
import org.hibernate.search.backend.FlushLuceneWork;
import org.hibernate.search.backend.LuceneWork;
import org.hibernate.search.backend.OptimizeLuceneWork;
import org.hibernate.search.backend.PurgeAllLuceneWork;
import org.hibernate.search.backend.UpdateLuceneWork;
import org.hibernate.search.backend.impl.WorkVisitor;
import org.infinispan.query.backend.KeyTransformationHandler;

/**
 * The serialized form of LuceneWork needs to be adjusted after deserialization to apply
 * our custom keyTransformers. LuceneWork instances are immutable, so we have to replace them
 * with new instances iff an id transformation is needed.
 * 
 * @author Sanne Grinovero <sanne@hibernate.org> (C) 2012 Red Hat Inc.
 */
public class LuceneWorkTransformationVisitor implements WorkVisitor<LuceneWorkIdTransformer> {

   static final LuceneWorkTransformationVisitor INSTANCE = new LuceneWorkTransformationVisitor();

   private static final LuceneWorkIdTransformer<AddLuceneWork> addDuplicator = new AddWorkDuplicator();
   private static final LuceneWorkIdTransformer<DeleteLuceneWork> deleteDuplicator = new DeleteWorkDuplicator();
   private static final LuceneWorkIdTransformer<UpdateLuceneWork> updateDuplicator = new UpdateWorkDuplicator();
   private static final LuceneWorkIdTransformer<LuceneWork> returnSameDuplicator = new NotReallyDuplicator();

   private LuceneWorkTransformationVisitor() {
      //no instances needed
   }

   @Override
   public LuceneWorkIdTransformer getDelegate(AddLuceneWork addLuceneWork) {
      return addDuplicator;
   }

   @Override
   public LuceneWorkIdTransformer getDelegate(DeleteLuceneWork deleteLuceneWork) {
      return deleteDuplicator;
   }

   @Override
   public LuceneWorkIdTransformer getDelegate(UpdateLuceneWork updateLuceneWork) {
      return updateDuplicator;
   }

   @Override
   public LuceneWorkIdTransformer getDelegate(OptimizeLuceneWork optimizeLuceneWork) {
      return returnSameDuplicator;
   }

   @Override
   public LuceneWorkIdTransformer getDelegate(FlushLuceneWork flushLuceneWork) {
      return returnSameDuplicator;
   }

   @Override
   public LuceneWorkIdTransformer getDelegate(PurgeAllLuceneWork purgeAllLuceneWork) {
      return returnSameDuplicator;
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
