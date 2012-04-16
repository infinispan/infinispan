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
package org.infinispan.configuration.parsing;

/**
 * Namespace.
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
public class Namespace {
   public static final String INFINISPAN_NS_BASE_URI = "urn:infinispan:config";
   private final String base;
   private final String rootElement;
   private final int major;
   private final int minor;

   public Namespace(final String rootElement) {
      this("", rootElement, 0, 0);
   }

   public Namespace(final String base, final String rootElement, int major, int minor) {
      this.base = base;
      this.rootElement = rootElement;
      this.major = major;
      this.minor = minor;
   }

   public String getBase() {
      return base;
   }

   public String getRootElement() {
      return rootElement;
   }

   public int getMajor() {
      return major;
   }

   public int getMinor() {
      return minor;
   }

   public String getUri() {
      return base == "" ? base : (base + ":" + major + "." + minor);
   }

   @Override
   public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((base == null) ? 0 : base.hashCode());
      result = prime * result + major;
      result = prime * result + minor;
      result = prime * result + ((rootElement == null) ? 0 : rootElement.hashCode());
      return result;
   }

   @Override
   public boolean equals(Object obj) {
      if (this == obj)
         return true;
      if (obj == null)
         return false;
      if (getClass() != obj.getClass())
         return false;
      Namespace other = (Namespace) obj;
      if (base == null) {
         if (other.base != null)
            return false;
      } else if (!base.equals(other.base))
         return false;
      if (major != other.major)
         return false;
      if (minor != other.minor)
         return false;
      if (rootElement == null) {
         if (other.rootElement != null)
            return false;
      } else if (!rootElement.equals(other.rootElement))
         return false;
      return true;
   }
}
