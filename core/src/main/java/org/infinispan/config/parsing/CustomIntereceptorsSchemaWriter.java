/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2009, Red Hat Middleware LLC, and individual contributors
 * as indicated by the @author tags. See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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
package org.infinispan.config.parsing;

import org.infinispan.config.parsing.ConfigurationElementWriter;
import org.infinispan.config.parsing.TreeNode;
import org.w3c.dom.Document;

public class CustomIntereceptorsSchemaWriter implements ConfigurationElementWriter{

   public void process(TreeNode node, Document c) {
      
      /*<xs:complexType name="customInterceptorsType">
      <xs:sequence>
         <xs:element name="interceptor" maxOccurs="unbounded">
            <xs:complexType>
               <xs:sequence>
                  <xs:element name="property" maxOccurs="unbounded" type="tns:propertyType" minOccurs="0"/>
               </xs:sequence>
               <xs:attribute name="class" type="xs:string"/>
               <xs:attribute name="position">
                  <xs:simpleType>
                     <xs:restriction base="xs:string">
                        <xs:pattern value="[Ff][Ii][Rr][Ss][Tt]|[Ll][Aa][Ss][Tt]"/>
                     </xs:restriction>
                  </xs:simpleType>
               </xs:attribute>
               <xs:attribute name="before" type="xs:string"/>
               <xs:attribute name="after" type="xs:string"/>
               <xs:attribute name="index" type="tns:positiveNumber"/>
            </xs:complexType>
         </xs:element>
      </xs:sequence>
   </xs:complexType>*/
   }

}
