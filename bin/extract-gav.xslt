<?xml version="1.0" encoding="UTF-8"?>
<xsl:stylesheet version="1.0"
    xmlns:pom="http://maven.apache.org/POM/4.0.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">

    <xsl:output method="text" indent="no" omit-xml-declaration="yes"/>
    <xsl:strip-space elements="*"/>

    <xsl:template match="/pom:project">
        <xsl:element name="artifact">
            <xsl:apply-templates select="pom:groupId|pom:parent/pom:groupId" mode="copy-coordinate"/>
            <xsl:text>:</xsl:text>
            <xsl:apply-templates select="pom:artifactId|pom:parent/pom:artifactId" mode="copy-coordinate"/>
            <xsl:text>:</xsl:text>
            <xsl:apply-templates select="pom:packaging|pom:parent/pom:packaging" mode="copy-coordinate"/>
            <xsl:text>:</xsl:text>
            <xsl:apply-templates select="pom:version|pom:parent/pom:version" mode="copy-coordinate"/>
            <xsl:text>:compile&#xa;</xsl:text>
        </xsl:element>
    </xsl:template>

    <xsl:template match="*" mode="copy-coordinate">
        <xsl:if test="not(../../*[name(.)=name(current())])">
            <xsl:element name="{local-name()}">
                <xsl:value-of select="."></xsl:value-of>
            </xsl:element>
        </xsl:if>
    </xsl:template>

</xsl:stylesheet>
