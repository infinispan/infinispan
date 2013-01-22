/* 
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors
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
package org.infinispan.lucene.cachestore;

import org.infinispan.config.ConfigurationBeanVisitor;
import org.infinispan.loaders.CacheLoaderConfig;
import org.infinispan.lucene.InfinispanDirectory;

/**
 * Configuration for a {@link LuceneCacheLoader}.
 * 
 * @author Sanne Grinovero
 * @since 5.2
 */
public final class LuceneCacheLoaderConfig implements CacheLoaderConfig {

   public static final String LOCATION_OPTION = "location";

   public static final String AUTO_CHUNK_SIZE_OPTION = "autoChunkSize";

   /**
    * Auto split huge files in blocks, by default of 32MB
    */
   protected int autoChunkSize = Integer.MAX_VALUE / 64;

   /**
    * Path of the root directory containing all indexes
    */
   protected String location = "Infinispan-IndexStore";

   /**
    * Path to the root directory containing all indexes. Indexes are loaded from the immediate subdirectories
    * of specified path, and each such subdirectory name will be the index name that must match the name
    * parameter of a {@link InfinispanDirectory} constructor.
    * 
    * @param location path to the root directory of all indexes
    * @return this for method chaining
    */
   public LuceneCacheLoaderConfig location(String location) {
      this.location = location;
      return this;
   }

   /**
    * When segment files are larger than this amount of bytes, the segment will be splitted in multiple chunks
    * if this size.
    * 
    * @param autoChunkSize
    * @return this for method chaining
    */
   public LuceneCacheLoaderConfig autoChunkSize(int autoChunkSize) {
      this.autoChunkSize = autoChunkSize;
      return this;
   }

   @Override
   public void accept(ConfigurationBeanVisitor visitor) {
      visitor.visitCacheLoaderConfig(this);
   }

   @Override
   public String getCacheLoaderClassName() {
      return LuceneCacheLoader.class.getName();
   }

   @Override
   public void setCacheLoaderClassName(String s) {
      //ignored
   }

   @Override
   public ClassLoader getClassLoader() {
      //we'll only need classes from this same module
      return LuceneCacheLoaderConfig.class.getClassLoader();
   }

   @Override
   public CacheLoaderConfig clone() {
      LuceneCacheLoaderConfig copy = new LuceneCacheLoaderConfig();
      copy.autoChunkSize = autoChunkSize;
      copy.location = location;
      return copy;
   }

}
