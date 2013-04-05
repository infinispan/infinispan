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
package org.infinispan.loaders.remote.wrapper;

import org.infinispan.client.hotrod.MetadataValue;
import org.infinispan.loaders.CacheLoaderException;
import org.infinispan.loaders.remote.logging.Log;
import org.infinispan.server.core.CacheValue;
import org.infinispan.util.logging.LogFactory;

/**
 * HotRodEntryWrapper.
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
public class HotRodEntryWrapper implements EntryWrapper<byte[], CacheValue> {
   private static final Log log = LogFactory.getLog(HotRodEntryWrapper.class, Log.class);

   @Override
   public byte[] wrapKey(Object key) throws CacheLoaderException {
      return (byte[]) key;
   }

   @Override
   public CacheValue wrapValue(MetadataValue<?> value) throws CacheLoaderException {
      Object v = value.getValue();
      if (v instanceof byte[]) {
         return new CacheValue((byte[]) v, value.getVersion());
      } else {
         throw log.unsupportedValueFormat(v != null ? v.getClass().getName() : "null");
      }
   }

}
