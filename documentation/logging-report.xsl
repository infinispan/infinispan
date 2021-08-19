<xsl:stylesheet version="2.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
                xmlns:xs="http://www.w3.org/2001/XMLSchema">
    <xsl:output method="html" indent="yes"/>
    <xsl:param name="directory" required="yes" as="xs:string"/>

    <xsl:template match="@*|node()">
        <xsl:copy>
            <xsl:apply-templates select="@*|node()"/>
        </xsl:copy>
    </xsl:template>

    <xsl:template match="/html/body/table/placeholder">
        <xsl:for-each select="collection(concat($directory,'?select=*.xml;recurse=yes'))//report/logs/log">
            <tr>
                <td>
                    <xsl:value-of select="id"/>
                </td>
                <td>
                    <xsl:value-of select="message"/>
                </td>
                <td>
                    <xsl:value-of select="level"/>
                </td>
                <td>
                    <xsl:value-of select="description"/>
                </td>
            </tr>
        </xsl:for-each>
    </xsl:template>
</xsl:stylesheet>

