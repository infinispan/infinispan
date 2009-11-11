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

        <h1>Enter Virus GBAN</h1>
        <form action="/InfinispanEC2WebDemo/CacheSearchServlet" method="POST">
            Genbank Accession Number
            <INPUT TYPE="TEXT" NAME="vGBAN">
            <INPUT type="submit" value="Go" />
        </form>
    </body>
</html>
