<?xml version="1.0" encoding="UTF-8"?>

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
