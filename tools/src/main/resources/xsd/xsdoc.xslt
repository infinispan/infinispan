<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<!-- ~ Copyright 2012 Red Hat, Inc. and/or its affiliates. ~ ~ This is free software; you can redistribute it and/or modify it ~ under the terms of the GNU Lesser General Public License as ~ published by the Free Software Foundation; either version 2.1 of ~ the License, or (at your option) any later 
   version. ~ ~ This software is distributed in the hope that it will be useful, ~ but WITHOUT ANY WARRANTY; without even the implied warranty of ~ MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU ~ Lesser General Public License for more details. ~ ~ You should have received a copy of the 
   GNU Lesser General Public ~ License along with this library; if not, write to the Free Software ~ Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA ~ 02110-1301 USA -->

<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform" xmlns="http://www.w3.org/1999/xhtml" xmlns:html="http://www.w3.org/1999/xhtml" xmlns:xs="http://www.w3.org/2001/XMLSchema" version="1.0" exclude-result-prefixes="xs html">
   <xsl:output method="xml" encoding="ISO-8859-1" standalone="yes" version="1.0" doctype-public="-//W3C//DTD XHTML 1.0 Strict//EN" doctype-system="http://www.w3.org/TR/xhtml1/DTD/xhtml1-strict.dtd" indent="yes" />
   
   <xsl:key name="simpleTypes" match="/xs:schema/xs:simpleType" use="@name"/>
   <xsl:key name="complexTypes" match="/xs:schema/xs:complexType" use="@name"/>
   <xsl:key name="imports" match="/xs:schema/xs:import" use="@namespace"/>

   
   <!-- Root -->
   <xsl:template match="/xs:schema">
      
      <html>
         <head>
            <title>
               <xsl:value-of select="@targetNamespace"/>
            </title>
            <style>
               body { font-family: 'sans-serif'; }
               div.element, div.complexType { margin: 0 0 1em 1em; border-left: 5px solid #4477aa; border-bottom: 2px groove #4477aa;}
               table { border: 1px solid red; border-collapse: collapse; margin: 5px; width: 95%; }
               th { text-align: left; background: #eeeeee; color: #773333; }
               td, th { border: 1px solid gray; }
               h3 { border: 1px solid #4477aa; background: #eeeeee; color: #4477aa; margin: 0.3em 0 0 0; padding: 0.3em; }
               a { text-decoration: none; color: #4477aa; }
               a:hover { color: white; background: #4477aa; }
               .error { color: white; background: red; }
               .content { }
               .heading { cursor: pointer; }
               .complexType, .simpleType {  }
               table.enumeration { border: 0; margin: 0; }
               #global {
                  position: fixed;
                  top: 1em;
                  right: 1em;
                  border: 1px solid #4477aa;
                  border-bottom: 2px groove #4477aa;
                  z-index: 100;
                  background: white;
                  padding: 0.5em;
               }
            </style>
            <script type="text/javascript" src="http://ajax.googleapis.com/ajax/libs/jquery/1.9.1/jquery.min.js">//</script>
            <script type="text/javascript">
               jQuery(document).ready(function() {
                 jQuery(".heading").click(function() {
                   jQuery(this).next(".content").slideToggle(500);
                 });
                 jQuery("#global a").click(function() {
                   jQuery(".content").toggle();
                 });
               });
            </script>
         </head>
         <body>
            <h1>
               <xsl:value-of select="@targetNamespace"/>
            </h1>
            
            <xsl:apply-templates select="xs:element" />
            
            <div id="global">
               <a href="#">Expand/Collapse All</a>
            </div>
         </body>
      </html>
   </xsl:template>

   <xsl:template match="xs:element">
      <div class="element">
         <h3 class="heading">
            <xsl:value-of select="@name" />
            <xsl:apply-templates select="." mode="occurs" />
         </h3>
         <div class="content">
            <p><xsl:apply-templates select="xs:annotation" /></p>
            <xsl:choose>
               <xsl:when test="@type">
                  <xsl:variable name="ref" select="substring-after(string(@type), ':')" />
                  <xsl:apply-templates select="key('complexTypes',$ref)" />
               </xsl:when>
               <xsl:otherwise>
                  <xsl:apply-templates select="xs:complexType" />
               </xsl:otherwise>
            </xsl:choose>
         </div>
      </div>
   </xsl:template>

   <xsl:template match="xs:complexType">
      <xsl:apply-templates select="xs:annotation" />

      <xsl:if test="xs:attribute">
         <table>
            <tr>
               <th>Name</th>
               <th>Type</th>
               <th>Default</th>
               <th>Description</th>
            </tr>
            <xsl:apply-templates select="xs:attribute" />
         </table>
      </xsl:if>
      <xsl:if test="xs:all | xs:sequence | xs:complexContent">
         <xsl:apply-templates select="xs:all | xs:sequence | xs:complexContent" />
      </xsl:if>
   </xsl:template>
   
   <xsl:template match="xs:complexContent">
      <xsl:apply-templates select="xs:extension"/>
   </xsl:template>

   <xsl:template match="xs:extension">
      <xsl:apply-templates select="xs:annotation" />

      <xsl:if test="xs:attribute">
         <table>
            <tr>
               <th>Name</th>
               <th>Type</th>
               <th>Default</th>
               <th>Description</th>
            </tr>
            <xsl:apply-templates select="xs:attribute" />
         </table>
      </xsl:if>
      <xsl:if test="xs:all | xs:sequence | xs:complexContent">
         <xsl:apply-templates select="xs:all | xs:sequence | xs:complexContent" />
      </xsl:if>
      <xsl:variable name="ns" select="substring-before(string(@base), ':')" />
      <xsl:variable name="ref" select="substring-after(string(@base), ':')" />
      <div>
         <xsl:apply-templates select="/xs:schema" mode="lookup-type">
            <xsl:with-param name="type" select="string(@base)" />
         </xsl:apply-templates>
      </div>
   </xsl:template>
   
   <xsl:template match="xs:schema" mode="lookup-type">
      <xsl:param name="type" />
      <xsl:variable name="ns" select="substring-before(string($type), ':')" />
      <xsl:variable name="ref" select="substring-after(string($type), ':')" />
      <xsl:choose>
         <xsl:when test="key('complexTypes',$ref)">
            <xsl:apply-templates select="key('complexTypes',$ref)" />
         </xsl:when>
         <xsl:otherwise>
            <xsl:for-each select="/xs:schema/xs:import">
               <xsl:apply-templates select="document(@schemaLocation, .)/xs:schema" mode="lookup-type">
                  <xsl:with-param name="type" select="$type"/>
               </xsl:apply-templates>
            </xsl:for-each>
         </xsl:otherwise>
      </xsl:choose>
      
      
   </xsl:template>
   
   <xsl:template match="xs:complexType" mode="top-level">
      <div class="complexType">
         <a>
            <xsl:attribute name="id"><xsl:value-of select="@name" /></xsl:attribute>
            <h3><xsl:value-of select="@name" /></h3>
         </a>
         <xsl:apply-templates select="." />
      </div>
   </xsl:template>

   <xsl:template match="xs:all">
      <xsl:apply-templates select="xs:element" />
   </xsl:template>

   <xsl:template match="xs:any">
      <xsl:apply-templates select="xs:element" />
   </xsl:template>

   <xsl:template match="xs:sequence">
      <xsl:apply-templates select="xs:element" />
   </xsl:template>

   <xsl:template match="xs:attribute">
      <tr>
         <td>
            <xsl:value-of select="@name" />
         </td>
         <td>
            <xsl:choose>
               <xsl:when test="xs:simpleType">
                  <xsl:apply-templates select="xs:simpleType" mode="embedded"/>
               </xsl:when>
               <xsl:when test="@type">
                  <xsl:call-template name="print-type">
                     <xsl:with-param name="ref" select="@type" />
                  </xsl:call-template>
               </xsl:when>
               <xsl:otherwise>
                  <span class="error">FIXME</span>
               </xsl:otherwise>
            </xsl:choose>
         </td>
         <td>
            <xsl:if test="@default">
               <xsl:value-of select="@default"></xsl:value-of>
            </xsl:if>
         </td>
         <td>
            <xsl:apply-templates select="xs:annotation" />
         </td>
      </tr>
   </xsl:template>

   <xsl:template match="xs:annotation">
      <xsl:for-each select="xs:documentation">
         <xsl:call-template name="format-text">
            <xsl:with-param name="text" select="."/>
         </xsl:call-template>
      </xsl:for-each>
   </xsl:template>

   <xsl:template name="print-type">
      <xsl:param name="ref" />

      <xsl:choose>
         <xsl:when test="starts-with($ref, 'xs:')">
            <!-- standard XSD types -->
            <xsl:value-of select="substring-after($ref, ':')" />
         </xsl:when>
         <xsl:otherwise>
            <xsl:apply-templates select="key('simpleTypes', substring-after($ref, ':'))" mode="embedded"/>
         </xsl:otherwise>
      </xsl:choose>
   </xsl:template>
   
   <xsl:template match="xs:simpleType" mode="embedded">
      <xsl:apply-templates select="xs:restriction" />
   </xsl:template>

   <xsl:template match="xs:simpleType" mode="top-level">
      <div class="simpleType">
         <a>
            <xsl:attribute name="id"><xsl:value-of select="@name" /></xsl:attribute>
            <h3>
               <xsl:value-of select="@name" />
            </h3>
         </a>
         <xsl:apply-templates select="xs:restriction" />
      </div>
   </xsl:template>

   <xsl:template match="xs:restriction">
      <xsl:if test="xs:enumeration">
         <table class="enumeration">
            <xsl:apply-templates select="xs:enumeration" />
         </table>
      </xsl:if>
   </xsl:template>
   
   <xsl:template match="xs:enumeration">
      <tr>
         <td>
            <xsl:attribute name="title"><xsl:apply-templates select="xs:annotation" /></xsl:attribute>
            <xsl:value-of select="@value" />
         </td>
      </tr>
   </xsl:template>

   <xsl:template match="xs:element | xs:any | xs:all | xs:sequence | xs:choice | xs:group" mode="occurs">
      <xsl:variable name="minOccurs">
         <xsl:choose>
            <xsl:when test="@minOccurs = 'unbounded'">*</xsl:when>
            <xsl:when test="@minOccurs">
               <xsl:value-of select="@minOccurs" />
            </xsl:when>
            <xsl:otherwise>1</xsl:otherwise>
         </xsl:choose>
      </xsl:variable>
      <xsl:variable name="maxOccurs">
         <xsl:choose>
            <xsl:when test="@maxOccurs = 'unbounded'">*</xsl:when>
            <xsl:when test="@maxOccurs">
               <xsl:value-of select="@maxOccurs" />
            </xsl:when>
            <xsl:otherwise>1</xsl:otherwise>
         </xsl:choose>
      </xsl:variable>
      <xsl:choose>
         <xsl:when test="$minOccurs = '0' and $maxOccurs = '1'">?</xsl:when>
         <xsl:when test="$minOccurs = '1' and $maxOccurs = '1'"></xsl:when>
         <xsl:when test="$minOccurs = '0' and $maxOccurs = '*'">*</xsl:when>
         <xsl:when test="$minOccurs = '1' and $maxOccurs = '*'">+</xsl:when>
         <xsl:otherwise>
            <xsl:value-of select="concat('{', $minOccurs, ',', $maxOccurs, '}')" />
         </xsl:otherwise>
      </xsl:choose>
   </xsl:template>

   <xsl:template name="format-text">
      <xsl:param name="text" />
      <xsl:choose>
         <xsl:when test="contains($text, 'http://')">
            <xsl:variable name="after_scheme" select="substring-after($text, 'http://')" />
            <xsl:value-of select="substring-before($text, 'http://')" />
            <xsl:choose>
               <xsl:when test="contains($after_scheme, ' ')">
                  <xsl:variable name="url" select="concat('http://', substring-before($after_scheme, ' '))" />
                  <xsl:call-template name="linkify">
                     <xsl:with-param name="url" select="$url" />
                  </xsl:call-template>
                  <xsl:text> </xsl:text>
                  <xsl:call-template name="format-text">
                     <xsl:with-param name="text" select="substring-after($after_scheme, ' ')" />
                  </xsl:call-template>
               </xsl:when>
               <xsl:otherwise>
                  <xsl:variable name="url" select="concat('http://', $after_scheme)" />
                  <xsl:call-template name="linkify">
                     <xsl:with-param name="url" select="$url" />
                  </xsl:call-template>
               </xsl:otherwise>
            </xsl:choose>
         </xsl:when>
         <xsl:when test="contains($text, 'https://')">
            <xsl:variable name="after_scheme" select="substring-after($text, 'https://')" />
            <xsl:value-of select="substring-before($text, 'https://')" />
            <xsl:choose>
               <xsl:when test="contains($after_scheme, ' ')">
                  <xsl:variable name="url" select="concat('https://', substring-before($after_scheme, ' '))" />
                  <xsl:call-template name="linkify">
                     <xsl:with-param name="url" select="$url" />
                  </xsl:call-template>
                  <xsl:text> </xsl:text>
                  <xsl:call-template name="format-text">
                     <xsl:with-param name="text" select="substring-after($after_scheme, ' ')" />
                  </xsl:call-template>
               </xsl:when>
               <xsl:otherwise>
                  <xsl:variable name="url" select="concat('https://', $after_scheme)" />
                  <xsl:call-template name="linkify">
                     <xsl:with-param name="url" select="$url" />
                  </xsl:call-template>
               </xsl:otherwise>
            </xsl:choose>
         </xsl:when>
         <xsl:otherwise>
            <xsl:value-of select="$text" />
         </xsl:otherwise>
      </xsl:choose>
   </xsl:template>
   
   <xsl:template name="linkify">
      <xsl:param name="url" />
      
      <a>
         <xsl:attribute name="href">
            <xsl:value-of select="$url" />
         </xsl:attribute>
         <xsl:value-of select="$url" />
      </a>
   </xsl:template>
</xsl:stylesheet>
