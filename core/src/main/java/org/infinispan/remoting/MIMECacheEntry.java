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
package org.infinispan.remoting;

import java.io.Serializable;
import java.util.Arrays;

/**
 * Wrapper object for entries that arrive via RESTful PUT/POST interface.
 * @author Michael Neale
 * @since 4.0
 */
public class MIMECacheEntry implements Serializable {

   private static final long serialVersionUID = -7857224258673285445L;

   /**
     * The MIME <a href="http://en.wikipedia.org/wiki/MIME">Content type</a>
     * value, for example application/octet-stream.
     * Often used in HTTP headers.
     */
    public String contentType;


    /**
     * The payload. The actual form of the contents depends on the contentType field.
     * Will be String data if the contentType is application/json, application/xml or text/*
     */
    public byte[] data;

    public MIMECacheEntry() {}

    public MIMECacheEntry(String contentType, byte[] data) {
        this.contentType = contentType;
        this.data = data;
    }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof MIMECacheEntry)) return false;

      MIMECacheEntry that = (MIMECacheEntry) o;

      return !(contentType != null ? !contentType.equals(that.contentType) : that.contentType != null) && Arrays.equals(data, that.data);
   }

   @Override
   public int hashCode() {
      return 31 * (contentType != null ? contentType.hashCode() : 0) + (data != null ? Arrays.hashCode(data) : 0);
   }
}
