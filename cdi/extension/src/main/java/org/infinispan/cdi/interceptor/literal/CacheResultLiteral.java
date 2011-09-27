/*
 * JBoss, Home of Professional Open Source
 * Copyright 2011 Red Hat Inc. and/or its affiliates and other
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
package org.infinispan.cdi.interceptor.literal;

import javax.cache.interceptor.CacheKeyGenerator;
import javax.cache.interceptor.CacheResolverFactory;
import javax.cache.interceptor.CacheResult;
import javax.enterprise.util.AnnotationLiteral;

/**
 * @author Kevin Pollet <kevin.pollet@serli.com> (C) 2011 SERLI
 */
public class CacheResultLiteral extends AnnotationLiteral<CacheResult> implements CacheResult {

   public final static CacheResultLiteral INSTANCE = new CacheResultLiteral();

   private CacheResultLiteral() {
   }

   @Override
   public String cacheName() {
      return "";
   }

   @Override
   public boolean skipGet() {
      return false;
   }

   @Override
   public Class<? extends CacheResolverFactory> cacheResolverFactory() {
      return CacheResolverFactory.class;
   }

   @Override
   public Class<? extends CacheKeyGenerator> cacheKeyGenerator() {
      return CacheKeyGenerator.class;
   }
}
