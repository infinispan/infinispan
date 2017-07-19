<?xml version="1.0"?>
<xsl:stylesheet version="1.0"
                xmlns:xsl="http://www.w3.org/1999/XSL/Transform">

    <xsl:output method="html" encoding="utf-8" standalone="no" media-type="text/html" />
    <xsl:param name="product"/>
    <xsl:param name="version"/>
    <xsl:variable name="lowercase" select="'abcdefghijklmnopqrstuvwxyz'" />
    <xsl:variable name="uppercase" select="'ABCDEFGHIJKLMNOPQRSTUVWXYZ'" />

    <xsl:template match="/">
        <html>
            <head>
                <meta http-equiv="Content-Type" content="text/html;charset=utf-8" />
                <link rel="stylesheet" type="text/css" href="licenses.css"/>
            </head>
            <body>
                <h2><xsl:value-of select="$product"/><xsl:text> </xsl:text><xsl:value-of select="$version"/></h2>
                <p>The following material has been provided for informational purposes only, and should not be relied upon or construed as a legal opinion or legal advice.</p>
                <!-- Read matching templates -->
                <table>
                    <tr>
                        <th>Package</th>
                        <th>Package Artifact</th>
                        <th>Package Version</th>
                        <th>Remote licenses</th>
                        <th>Local licenses</th>
                    </tr>
                    <xsl:for-each select="licenseSummary/dependencies/dependency">
                        <xsl:sort select="concat(groupId, '.', artifactId)"/>
                        <tr>
                            <td><xsl:value-of select="concat(groupId, packageName)"/></td>
                            <td><xsl:value-of select="artifactId"/></td>
                            <td><xsl:value-of select="version"/></td>
                            <td>
                                <xsl:for-each select="licenses/license">
                                    <a href="{./url}"><xsl:value-of select="name"/></a><br/>
                                </xsl:for-each>
                            </td>
                            <td>
                                <xsl:for-each select="licenses/license">
                                    <xsl:variable name="name" select="translate(name,$uppercase,$lowercase)" />
                                    <xsl:variable name="last-index">
                                        <xsl:call-template name="last-index-of">
                                            <xsl:with-param name="txt" select="url"/>
                                            <xsl:with-param name="delimiter" select="'/'"></xsl:with-param>
                                        </xsl:call-template>
                                    </xsl:variable>
                                    <xsl:variable name="prefix" select="concat($name,' - ')" />
                                    <xsl:variable name="postfix" select="substring(url,$last-index+1)" />
                                    <xsl:variable name="filename">
                                        <xsl:call-template name="remap-local-filename">
                                            <xsl:with-param name="filename" select="concat($prefix,translate($postfix,$uppercase,$lowercase))" />
                                        </xsl:call-template>
                                    </xsl:variable>
                                    <a href="{$filename}"><xsl:value-of select="$filename"/></a><br/>
                                </xsl:for-each>
                            </td>
                        </tr>
                    </xsl:for-each>
                </table>
            </body>
        </html>
    </xsl:template>

    <xsl:template name="last-index-of">
        <xsl:param name="txt"/>
        <xsl:param name="remainder" select="$txt"/>
        <xsl:param name="delimiter" select="' '"/>

        <xsl:choose>
            <xsl:when test="contains($remainder, $delimiter)">
                <xsl:call-template name="last-index-of">
                    <xsl:with-param name="txt" select="$txt"/>
                    <xsl:with-param name="remainder" select="substring-after($remainder, $delimiter)"/>
                    <xsl:with-param name="delimiter" select="$delimiter"/>
                </xsl:call-template>
            </xsl:when>
            <xsl:otherwise>
                <xsl:variable name="lastIndex" select="string-length(substring($txt, 1, string-length($txt)-string-length($remainder)))+1"/>
                <xsl:choose>
                    <xsl:when test="string-length($remainder)=0">
                        <xsl:value-of select="string-length($txt)"/>
                    </xsl:when>
                    <xsl:when test="$lastIndex>0">
                        <xsl:value-of select="($lastIndex - string-length($delimiter))"/>
                    </xsl:when>
                    <xsl:otherwise>
                        <xsl:value-of select="0"/>
                    </xsl:otherwise>
                </xsl:choose>
            </xsl:otherwise>
        </xsl:choose>
    </xsl:template>
    <xsl:template name="remap-local-filename">
        <xsl:param name="filename"/>

        <xsl:choose>
            <xsl:when test="contains($filename, 'apache') and contains($filename, 'version 1.1')">
                <xsl:text>Apache-Software-License-version-1.1.txt</xsl:text>
            </xsl:when>
            <xsl:when test="contains($filename, 'apache') and contains($filename, 'version 2.0')">
                <xsl:text>Apache-Software-License-version-2.0.txt</xsl:text>
            </xsl:when>
            <xsl:when test="contains($filename, 'bouncy')">
                <xsl:text>Bouncy-Castle-License.txt</xsl:text>
            </xsl:when>
            <xsl:when test="contains($filename, 'bsd') and contains($filename, '2-clause')">
                <xsl:text>BSD-2-Clause-License.txt</xsl:text>
            </xsl:when>
            <xsl:when test="contains($filename, 'bsd') and contains($filename, '3-clause')">
                <xsl:text>BSD-3-Clause-"New"-or-"Revised"-License.txt</xsl:text>
            </xsl:when>
            <xsl:when test="contains($filename, 'cddl 1.1') and contains($filename, 'gpl 2') and contains($filename, 'classpath')">
                <xsl:text>CDDL-1.1-and-GPL-2-with-Classpath-Exception.txt</xsl:text>
            </xsl:when>
            <xsl:when test="contains($filename, 'common development') and contains($filename, 'license 1.0')">
                <xsl:text>Common-Development-and-Distribution-License-1.0.txt</xsl:text>
            </xsl:when>
            <xsl:when test="contains($filename, 'common development') and contains($filename, 'license 1.0')">
                <xsl:text>Common-Development-and-Distribution-License-1.1.txt</xsl:text>
            </xsl:when>
            <xsl:when test="contains($filename, 'common public') and contains($filename, 'license 1.0')">
                <xsl:text>Common-Public-License-1.0.txt</xsl:text>
            </xsl:when>
            <xsl:when test="contains($filename, 'creative commons') and contains($filename, 'license 2.5')">
                <xsl:text>Creative-Commons-Attribution-License-2.5.txt</xsl:text>
            </xsl:when>
            <xsl:when test="contains($filename, 'dual license') and contains($filename, 'cddl v1.1') and contains($filename, 'gpl v2')">
                <xsl:text>Dual-license-consisting-of-the-CDDL-v1.1-and-GPL-v2.txt</xsl:text>
            </xsl:when>
            <xsl:when test="contains($filename, 'eclipse distribution') and contains($filename, 'version 1.0')">
                <xsl:text>Eclipse-Distribution-License,-Version-1.0.txt</xsl:text>
            </xsl:when>
            <xsl:when test="contains($filename, 'eclipse public') and contains($filename, '1.0')">
                <xsl:text>Eclipse-Public-License-1.0.txt</xsl:text>
            </xsl:when>
            <xsl:when test="contains($filename, 'gnu general public license') and contains($filename, 'classpath exception') and contains($filename, '2')">
                <xsl:text>GNU-General-Public-License,-Version-2-with-Classpath-Exception.txt</xsl:text>
            </xsl:when>
            <xsl:when test="contains($filename, 'gnu general public license') and contains($filename, 'version 3')">
                <xsl:text>GNU-General-Public-License,-Version-3.txt</xsl:text>
            </xsl:when>
            <xsl:when test="contains($filename, 'gnu general public license') and contains($filename, 'v3.0')">
                <xsl:text>GNU-General-Public-License,-Version-3.txt</xsl:text>
            </xsl:when>
            <xsl:when test="contains($filename, 'gnu lesser general public license') and contains($filename, 'version 2')">
                <xsl:text>GNU-Lesser-General-Public-License,-Version-2.txt</xsl:text>
            </xsl:when>
            <xsl:when test="contains($filename, 'gnu lesser general public license') and contains($filename, '2.1')">
                <xsl:text>GNU-Lesser-General-Public-License,-Version-2.1.txt</xsl:text>
            </xsl:when>
            <xsl:when test="contains($filename, 'gnu lesser general public license') and contains($filename, 'version 3')">
                <xsl:text>GNU-Lesser-General-Public-License,-Version-3.txt</xsl:text>
            </xsl:when>
            <xsl:when test="contains($filename, 'h2 license')">
                <xsl:text>H2-License.txt</xsl:text>
            </xsl:when>
            <xsl:when test="contains($filename, 'indianna university extreme')">
                <xsl:text>Indiana-University-Extreme-Lab-Software-License-vesion-1.1.1.txt</xsl:text>
            </xsl:when>
            <xsl:when test="contains($filename, 'isc license')">
                <xsl:text>ISC-License.txt</xsl:text>
            </xsl:when>
            <xsl:when test="contains($filename, 'jdom')">
                <xsl:text>JDOM-License.txt</xsl:text>
            </xsl:when>
            <xsl:when test="contains($filename, 'mit license')">
                <xsl:text>MIT-License.txt</xsl:text>
            </xsl:when>
            <xsl:when test="contains($filename, 'mozilla') and contains($filename, '1.1')">
                <xsl:text>Mozilla-Public-License-1.1.txt</xsl:text>
            </xsl:when>
            <xsl:when test="contains($filename, 'public domain')">
                <xsl:text>Public-Domain.txt</xsl:text>
            </xsl:when>
            <xsl:when test="contains($filename, 'sil') and contains($filename, 'font')">
                <xsl:text>SIL-OFL-1.1.txt</xsl:text>
            </xsl:when>
            <xsl:otherwise>
                <xsl:value-of select="N/A"/>
            </xsl:otherwise>
        </xsl:choose>
    </xsl:template>
</xsl:stylesheet>
