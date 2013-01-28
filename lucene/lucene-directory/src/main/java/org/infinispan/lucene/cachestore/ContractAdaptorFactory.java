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


/**
 * @author Sanne Grinovero
 * @since 5.2
 */
package org.infinispan.lucene.cachestore;

import org.apache.lucene.store.Directory;
import org.infinispan.lucene.impl.LuceneVersionDetector;
import org.infinispan.lucene.logging.Log;
import org.infinispan.util.logging.LogFactory;


/**
 * @since 5.2
 * @author Sanne Grinovero
 */
public class ContractAdaptorFactory {

   private static final Log log = LogFactory.getLog(ContractAdaptorFactory.class, Log.class);

   public static InternalDirectoryContract wrapNativeDirectory(Directory directory) {
      if (LuceneVersionDetector.VERSION == 3) {
         return new DirectoryV3Adaptor(directory);
      }
      else {
         Class<?>[] ctorType = new Class[]{ Directory.class };
         InternalDirectoryContract idc;
         try {
            idc = (InternalDirectoryContract) ContractAdaptorFactory.class.getClassLoader()
               .loadClass("org.infinispan.lucene.cachestore.DirectoryV4Adaptor")
               .getConstructor(ctorType)
               .newInstance(directory);
         } catch (Exception e) {
            throw log.failedToCreateLucene4Directory(e);
         }
         return idc;
      }
   }

}
