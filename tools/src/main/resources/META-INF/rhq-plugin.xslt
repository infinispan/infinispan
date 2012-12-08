<?xml version="1.0" encoding="UTF-8"?>

<xsl:stylesheet xmlns="urn:xmlns:rhq-plugin" xmlns:xsl="http://www.w3.org/1999/XSL/Transform" xmlns:c="urn:xmlns:rhq-configuration" xmlns:xslt="http://xml.apache.org/xslt" version="1.0">
   <xsl:output method="xml" indent="yes" version="1.0" encoding="UTF-8" omit-xml-declaration="no" standalone="yes" xslt:indent-amount="4"/>

   <xsl:template match="/plugin">
      <plugin name="Infinispan" displayName="Infinispan Plugin" description="Supports management and monitoring of Infinispan" package="org.infinispan.rhq" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns="urn:xmlns:rhq-plugin" xmlns:c="urn:xmlns:rhq-configuration">
         <depends plugin="JMX" useClasses="true" />
         <xsl:apply-templates select="cacheManager" />
      </plugin>
   </xsl:template>

   <xsl:template match="cacheManager">
      <service name="Infinispan Cache Manager" discovery="CacheManagerDiscovery" class="CacheManagerComponent" supportsManualAdd="true">
         <runs-inside>
            <parent-resource-type name="JBossAS Server" plugin="JBossAS" />
            <parent-resource-type name="JBossAS Server" plugin="JBossAS5" />
            <parent-resource-type name="JMX Server" plugin="JMX" />
         </runs-inside>
         <plugin-configuration>
            <c:simple-property name="name" description="Name" type="string" default="Infinispan Cache Manager" readOnly="true" />
         </plugin-configuration>
         <xsl:apply-templates select="operation" />
         <xsl:apply-templates select="metric" />
         <xsl:apply-templates select="/plugin/cache" />
      </service>
   </xsl:template>

   <xsl:template match="/plugin/cache">
      <service name="Infinispan Cache" discovery="CacheDiscovery" class="CacheComponent">
         <xsl:apply-templates select="operation" />
         <xsl:apply-templates select="metric" />
      </service>
   </xsl:template>

   <xsl:template match="operation">
      <xsl:copy>
         <xsl:apply-templates select="@*|node()" />
      </xsl:copy>
   </xsl:template>

   <xsl:template match="metric">
      <xsl:copy>
         <xsl:apply-templates select="@*|node()" />
      </xsl:copy>
   </xsl:template>

   <xsl:template match="@*|node()">
      <xsl:copy>
         <xsl:apply-templates select="@*|node()" />
      </xsl:copy>
   </xsl:template>

</xsl:stylesheet>
