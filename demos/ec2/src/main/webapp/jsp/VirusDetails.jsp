<%@ taglib uri="http://java.sun.com/jsp/jstl/core" prefix="c" %>
<%@ page import="org.infinispan.ec2demo.Influenza_N_P_CR_Element" %>
<%@ page import="org.infinispan.ec2demo.Nucleotide_Protein_Element" %>
<%-- 
    Document   : VirusDetails
    Created on : Nov 10, 2009, 7:32:55 AM
    Author     : noconnor@redhat.com
--%>

<%@page contentType="text/html" pageEncoding="UTF-8"%>
<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN"
   "http://www.w3.org/TR/html4/loose.dtd">

<html>
    <head>
        <meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
        <title>Influenza Details</title>
    </head>
    <body>
        <h1>Search Details for <c:out value="${total}"></c:out></h1>
      	        	
        	<h1>Nucleotide Information</h1>        	
       	  <table border="1">
            <thead>
                <tr>
                    <th>Virus Name</th>
                    <th>Host</th>
                    <th>Country</th>
                    <th>Year Found</th>
                    <th>GBAN</th>
                </tr>
            </thead>
            <tbody>
                <tr>
                    <td> <c:out value="${Nucleotide.virusName}" /></td>
                    <td> <c:out value="${Nucleotide.host}" /></td>
                    <td><c:out value="${Nucleotide.country}" /></td>
                    <td><c:out value="${Nucleotide.yearFound}" /></td>
                    <td><c:out value="${Nucleotide.genbankAccessionNumber}" /></td>
                </tr>
             </tbody>
        </table> 
        
                	<h1>Protein Information</h1>       
                	       	  <table border="1">
            <thead>
                <tr>
                    <th>Protein GBAN</th>
                    <th>Protein Coding Region</th>
                </tr>
            </thead>
            <tbody>
                <c:forEach items="${PMap}" var="protein">
                <tr>
                    <td> <c:out value="${protein.key}" /></td>
                    <td> <c:out value="${protein.value}" /></td>
                                    </tr>
                                        </c:forEach>
             </tbody>
        </table> 
        
        
                        	<h1>Cluster Information</h1>       
                	       	  <table border="1">
            <thead>
                <tr>
                    <th>Nodes</th>
                </tr>
            </thead>
            <tbody>
                <c:forEach items="${CMap}" var="node">
                <tr>
                    <td> <c:out value="${node}" /></td>
                </tr>
                </c:forEach>
             </tbody>
        </table> 
        
    </body>
</html>
