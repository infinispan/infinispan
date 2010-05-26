<%-- 
    Document   : VirusSearch
    Created on : Nov 10, 2009, 12:48:06 AM
    Author     : noconnor@redhat.com
--%>

<%@page contentType="text/html" pageEncoding="UTF-8"%>
<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN"
    "http://www.w3.org/TR/html4/loose.dtd">

<html>
<head>
<meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
<title>Virus Details Search</title>


</head>
<body>
<table class="contentTable" border="0" cellpadding="0" cellspacing="0"
	width="945">
	<tbody>
		<tr>
			<td class="header" colspan="2" align="left" width="945">
			<div id="header">
			<div class="paragraphInformationBoxLayout"
				style="height: 0px; display: none; visibility: hidden;"
				id="mgnlParagraph1">
			<div align="right">Inherited from: <a style="color: black;"
				title="/infinispan.html" href="http://jboss.org/infinispan.html">/infinispan.html</a></div>
			</div>
			<div id="commonContent">
			<div id="proj_announce-whole"><img alt="Infinispan"
				src="js/infinispan-banner.png">
			<div id="proj_logo-neg"></div>
			</div>
			</div>
			</div>
			</td>
		</tr>
	</tbody>
</table>

<h1>Enter Virus GBAN</h1>
<form action="/infinispan-ec2-demoui/CacheSearchServlet" method="POST">
Genbank Accession Number <INPUT TYPE="TEXT" NAME="vGBAN"> <INPUT
	type="submit" value="Go" /></form>
</body>
</html>
