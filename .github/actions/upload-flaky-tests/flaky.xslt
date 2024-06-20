<?xml version="1.0"?>
<xsl:stylesheet version="1.0"
    xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
<xsl:output method="text"/>
<xsl:template match="testsuite">
	<xsl:apply-templates select="testcase[flakyFailure|flakyError]"/>
</xsl:template>
<xsl:template match="testcase">
  - <xsl:value-of select="@name"/><xsl:text>  </xsl:text>
   <xsl:value-of select="@classname"/> 
</xsl:template>
</xsl:stylesheet>
