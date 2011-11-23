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
package org.infinispan;

import java.util.ArrayList;
import java.util.List;

/**
 * An exception that represents an error in the configuration.  This could be a parsing error or a logical error
 * involving clashing configuration options or missing mandatory configuration elements.
 *
 * @author <a href="mailto:manik@jboss.org">Manik Surtani (manik@jboss.org)</a>
 * 
 * @since 4.0
 */
public class CacheConfigurationException extends CacheException {
  
   private static final long serialVersionUID = -1907871321266402356L;
   
   private List<String> erroneousAttributes = new ArrayList<String>();

   public CacheConfigurationException(Exception e) {
      super(e);
   }

   public CacheConfigurationException(String string) {
      super(string);
   }

   public CacheConfigurationException(String string, String erroneousAttribute) {
      super(string);
      erroneousAttributes.add(erroneousAttribute);
   }

   public CacheConfigurationException(String string, Throwable throwable) {
      super(string, throwable);
   }

   public List<String> getErroneousAttributes() {
      return erroneousAttributes;
   }

   public void addErroneousAttribute(String s) {
      erroneousAttributes.add(s);
   }
}
