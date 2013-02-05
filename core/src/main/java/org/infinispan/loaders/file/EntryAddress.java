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

package org.infinispan.loaders.file;

/**
 * Encapsulates data entry parameters(Offset, cachefilename, length) in a single long variable
 * Offset is limited to 32 bits, thus Integer.MAX_VALUE
 * DataLen and FileName are limited to 16 bits
 * @author Patrick Azogni
 *
 */
public final class EntryAddress {

   private final int fileNameShift = 32;
   private final int fileNameMask = 0xffff;
   
   private final int dataLenShift = 48;
   private final int dataLenMask = 0xffff;
   
   private final int offsetMask = 0x7fffffff;

   public final int getOffset(long addr) {
      return (int)(addr & offsetMask);
   }

   public final String getFileName(long addr) {
      int fileName = ((int)(addr >> fileNameShift) & fileNameMask);
      return String.valueOf(fileName);
   }

   public final int getDataLen(long addr) {
      return ((int)(addr >> dataLenShift) & dataLenMask);
   }

   public final long composeAddress(int offset, String fileName, int dataSize) {
      int name = Integer.parseInt(fileName);
      return (offset | ((long)name << fileNameShift) | ((long)dataSize << dataLenShift));
   }

}
