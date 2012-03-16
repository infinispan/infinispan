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
<xsl:stylesheet version="1.0"
      xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
   <xsl:output method="xml"/>
   <xsl:param name="indent-increment" select="'   '" />

   <xsl:template match="*">
      <xsl:param name="indent" select="'&#xA;'"/>

      <xsl:value-of select="$indent"/>
      <xsl:copy>
        <xsl:copy-of select="@*" />
        <xsl:apply-templates>
          <xsl:with-param name="indent"
               select="concat($indent, $indent-increment)"/>
        </xsl:apply-templates>
        <xsl:if test="*">
          <xsl:value-of select="$indent"/>
        </xsl:if>
      </xsl:copy>
   </xsl:template>

   <xsl:template match="comment()|processing-instruction()">
      <xsl:copy />
   </xsl:template>
   <xsl:template match="text()[normalize-space(.)='']"/>

</xsl:stylesheet>