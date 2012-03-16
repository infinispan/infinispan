<?xml version="1.0" encoding="UTF-8"?>

<!--
  ~ JBoss, Home of Professional Open Source
  ~ Copyright 2009 Red Hat Inc. and/or its affiliates and other
  ~ contributors as indicated by the @author tags. All rights reserved.
  ~ See the copyright.txt in the distribution for a full listing of
  ~ individual contributors.
  ~
  ~ This is free software; you can redistribute it and/or modify it
  ~ under the terms of the GNU Lesser General Public License as
  ~ published by the Free Software Foundation; either version 2.1 of
  ~ the License, or (at your option) any later version.
  ~
  ~ This software is distributed in the hope that it will be useful,
  ~ but WITHOUT ANY WARRANTY; without even the implied warranty of
  ~ MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
  ~ Lesser General Public License for more details.
  ~
  ~ You should have received a copy of the GNU Lesser General Public
  ~ License along with this software; if not, write to the Free
  ~ Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
  ~ 02110-1301 USA, or see the FSF site: http://www.fsf.org.
  -->
<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform" version="1.0">

   <xsl:output method="xml" indent="yes" version="1.0" encoding="UTF-8" omit-xml-declaration="yes"
               xmlns="urn:jboss:jbosscache-core:config:3.0"/>

   <xsl:strip-space elements="*"/>

   <xsl:template match="/jbosscache/clustering/jgroupsConfig">
      <xsl:element name="config">
         <xsl:apply-templates mode="copy-no-ns" select="/jbosscache/clustering/jgroupsConfig/*"/>
      </xsl:element>

   </xsl:template>

   <xsl:template mode="copy-no-ns" match="*">
      <xsl:element name="{name(.)}" namespace="{namespace-uri(.)}">
         <xsl:copy-of select="@*"/>
         <xsl:apply-templates mode="copy-no-ns"/>
      </xsl:element>
   </xsl:template>


   <xsl:template match="/jbosscache/loaders"/>
</xsl:stylesheet>
