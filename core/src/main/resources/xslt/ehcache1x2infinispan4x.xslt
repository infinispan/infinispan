<?xml version="1.0" encoding="UTF-8"?>

<!--
  ~ JBoss, Home of Professional Open Source
  ~ Copyright 2010 Red Hat Inc. and/or its affiliates and other
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
<xsl:stylesheet xmlns="urn:infinispan:config:4.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform" version="1.0">
   <xsl:output method="xml" indent="yes" version="1.0" encoding="UTF-8" omit-xml-declaration="no"/>
   <xsl:template match="/ehcache">
      <xsl:comment>
         This XSL stylesheet is used to convert an EHCache 1.x configuration into an Infinispan 4.0.x configuration.
         Note that Infinispan supports JGroups only, caches are migrated to using JGroups.
         Peer discovery will also be using JGroups. Eviction policies are translated to LRU, FIFO or NONE.
      </xsl:comment>
      <xsl:element name="infinispan">

         <xsl:element name="global">
            <transport />
            <asyncListenerExecutor factory="org.infinispan.executors.DefaultExecutorFactory">
               <properties>
                  <property name="threadNamePrefix" value="AsyncTransportThread"/>
               </properties>
            </asyncListenerExecutor>

            <asyncTransportExecutor factory="org.infinispan.executors.DefaultExecutorFactory">
               <properties>
                 <property name="threadNamePrefix" value="AsyncListenerThread"/>
               </properties>
            </asyncTransportExecutor>

            <evictionScheduledExecutor factory="org.infinispan.executors.DefaultScheduledExecutorFactory">
               <properties>
                  <property name="threadNamePrefix" value="EvictionThread"/>
               </properties>
            </evictionScheduledExecutor>

            <replicationQueueScheduledExecutor factory="org.infinispan.executors.DefaultScheduledExecutorFactory">
               <properties>
                  <property name="threadNamePrefix" value="ReplicationQueueThread"/>
               </properties>
            </replicationQueueScheduledExecutor>

            <globalJmxStatistics jmxDomain="infinispan" enabled="true"/>

            <shutdown hookBehavior="DEFAULT"/>
         </xsl:element>

         <xsl:for-each select="defaultCache">
            <xsl:element name="default">
               <xsl:call-template name="generateCache"/>
            </xsl:element>
         </xsl:for-each>

         <xsl:for-each select="cache">
            <xsl:element name="namedCache">
               <xsl:attribute name="name">
                  <xsl:value-of select="@name"/>
               </xsl:attribute>
               <xsl:call-template name="generateCache"/>
            </xsl:element>
         </xsl:for-each>
      </xsl:element>
   </xsl:template>

   <xsl:template name="generateCache">
      <xsl:if test="@memoryStoreEvictionPolicy">
         <xsl:element name="eviction">
            <xsl:attribute name="strategy">
               <xsl:if test="contains(@memoryStoreEvictionPolicy, 'LRU') or contains(@memoryStoreEvictionPolicy, 'FIFO')">
                  <xsl:value-of select="@memoryStoreEvictionPolicy"/>
               </xsl:if>

               <xsl:if test="contains(@memoryStoreEvictionPolicy, 'LFU')">
                  <xsl:message terminate="no">WARNING!!! Infinispan does not support LFU eviction. Using LRU instead.
                  </xsl:message>
                  <xsl:text>LRU</xsl:text>
               </xsl:if>
            </xsl:attribute>

            <xsl:attribute name="wakeUpInterval">
               <xsl:choose>
                  <xsl:when test="@diskExpiryThreadIntervalSeconds">
                     <xsl:value-of select="concat(@diskExpiryThreadIntervalSeconds,'000')"/>
                  </xsl:when>
                  <xsl:otherwise>
                     <!-- by default the value is 120 seconds in EHCache-->
                     <xsl:value-of select="120000"/>
                  </xsl:otherwise>
               </xsl:choose>
            </xsl:attribute>

            <xsl:if test="@maxElementsInMemory">
               <xsl:attribute name="maxEntries">
                  <xsl:value-of select="@maxElementsInMemory"/>
               </xsl:attribute>
            </xsl:if>
         </xsl:element>
      </xsl:if>

      <xsl:if
            test="(@timeToIdleSeconds or @timeToLiveSeconds) and not(@eternal = 'true')">
         <xsl:element name="expiration">
            <xsl:if test="@timeToIdleSeconds">
               <xsl:attribute name="maxIdle">
                  <xsl:value-of select="@timeToIdleSeconds"/>
               </xsl:attribute>
            </xsl:if>
            <xsl:if test="@timeToLiveSeconds">
               <xsl:attribute name="lifespan">
                  <xsl:value-of select="@timeToLiveSeconds"/>
               </xsl:attribute>
            </xsl:if>
         </xsl:element>
      </xsl:if>

      <xsl:choose>
         <xsl:when test="@overflowToDisk='true'">
            <xsl:if test="/ehcache/defaultCache[@overflowToDisk = 'false'] or not(@name)">
               <xsl:element name="loaders">
                  <xsl:attribute name="passivation">true</xsl:attribute>
                  <xsl:attribute name="preload">
                     <xsl:value-of select="string(@diskPersistent = 'true')"/>
                  </xsl:attribute>
                  <xsl:element name="loader">
                     <xsl:attribute name="class">org.infinispan.loaders.file.FileCacheStore</xsl:attribute>
                     <xsl:attribute name="purgeOnStartup">
                        <xsl:value-of select="string(@diskPersistent = 'true')"/>
                     </xsl:attribute>
                     <xsl:if test="/ehcache/diskStore[@path]">
                        <properties>
                           <xsl:element name="property">
                              <xsl:attribute name="name">location</xsl:attribute>
                              <xsl:attribute name="value">
                                 <xsl:choose>
                                    <xsl:when
                                          test="/ehcache/diskStore[@path='user.home' or @path='user.dir' or @path='java.io.tmpdir']">
                                       <xsl:value-of select="concat('${',/ehcache/diskStore/@path,'}')"/>
                                    </xsl:when>
                                    <xsl:otherwise>
                                       <xsl:value-of select="/ehcache/diskStore/@path"/>
                                    </xsl:otherwise>
                                 </xsl:choose>
                              </xsl:attribute>
                           </xsl:element>
                        </properties>
                     </xsl:if>
                  </xsl:element>
               </xsl:element>
            </xsl:if>
         </xsl:when>
         <xsl:otherwise>
            <xsl:if test="/ehcache/defaultCache[@overflowToDisk = 'true']">
               <!-- if this cache does not overflow to disk then override the possible cache loader definition from 'defaultCache' -->
               <loaders/>
            </xsl:if>
         </xsl:otherwise>
      </xsl:choose>
      <xsl:if test="cacheEventListenerFactory">
         <xsl:element name="clustering">
            <xsl:attribute name="mode">
               <xsl:choose>
                  <xsl:when test="contains(cacheEventListenerFactory/@properties, 'replicatePutsViaCopy=false')">
                     <xsl:text>invalidation</xsl:text>
                  </xsl:when>
                  <xsl:otherwise>
                     <xsl:text>distribution</xsl:text>
                  </xsl:otherwise>
               </xsl:choose>
            </xsl:attribute>
            <xsl:choose>
               <xsl:when
                     test="contains(cacheEventListenerFactory/@properties, 'replicateAsynchronously=false')">
                  <sync/>
               </xsl:when>
               <xsl:otherwise>
                  <async/>
               </xsl:otherwise>
            </xsl:choose>
         </xsl:element>
      </xsl:if>
   </xsl:template>
</xsl:stylesheet>
