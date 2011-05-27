/*
 * JBoss, Home of Professional Open Source
 * Copyright 2009 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.infinispan.ec2demo;

import java.io.Serializable;


/**
 * @author noconnor@redhat.com
 * 
 */
public class Nucleotide_Protein_Element implements Serializable {


	/**
	 * 
	 */
	private static final long serialVersionUID = 2870569691958410447L;

	public String getGenbankAccessionNumber() {
		return GenbankAccessionNumber;
	}

	public void setGenbankAccessionNumber(String genbankAccessionNumber) {
		GenbankAccessionNumber = genbankAccessionNumber;
	}

	public String getHost() {
		return Host;
	}

	public void setHost(String host) {
		Host = host;
	}

	public String getGenomeSequenceNumber() {
		return GenomeSequenceNumber;
	}

	public void setGenomeSequenceNumber(String genomeSequenceNumber) {
		GenomeSequenceNumber = genomeSequenceNumber;
	}

	public String getSubType() {
		return SubType;
	}

	public void setSubType(String subType) {
		SubType = subType;
	}

	public String getCountry() {
		return country;
	}

	public void setCountry(String Country) {
		country = Country;
	}

	public int getYearFound() {
		return YearFound;
	}

	public void setYearFound(String yearFound) {
		try {
			YearFound = new Integer(yearFound);
		} catch (Exception ex) {
			YearFound = 1900;
		}
	}

	public int getSequenceLength() {
		return SequenceLength;
	}

	public void setSequenceLength(int sequenceLength) {
		SequenceLength = sequenceLength;
	}

	public String getVirusName() {
		return VirusName;
	}

	public void setVirusName(String virusName) {
		VirusName = virusName;
	}

	public String getHostAge() {
		return HostAge;
	}

	public void setHostAge(String hostAge) {
		HostAge = hostAge;
	}

	public String getHostGender() {
		return HostGender;
	}

	public void setHostGender(String hostGender) {
		HostGender = hostGender;
	}

	public Boolean getFullLengthSequence() {
		return FullLengthSequence;
	}

	public void setFullLengthSequence(String fullLengthSequence) {
		if ((fullLengthSequence.isEmpty()) || (fullLengthSequence.equalsIgnoreCase("F")))
			FullLengthSequence = false;
		else
			FullLengthSequence = true;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((country == null) ? 0 : country.hashCode());
		result = prime * result + ((FullLengthSequence == null) ? 0 : FullLengthSequence.hashCode());
		result = prime * result + ((GenbankAccessionNumber == null) ? 0 : GenbankAccessionNumber.hashCode());
		result = prime * result + GenomeSequenceNumber.hashCode();
		result = prime * result + ((Host == null) ? 0 : Host.hashCode());
		result = prime * result + ((HostAge == null) ? 0 : HostAge.hashCode());
		result = prime * result + ((HostGender == null) ? 0 : HostGender.hashCode());
		result = prime * result + SequenceLength;
		result = prime * result + ((SubType == null) ? 0 : SubType.hashCode());
		result = prime * result + ((VirusName == null) ? 0 : VirusName.hashCode());
		result = prime * result + YearFound;
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Nucleotide_Protein_Element other = (Nucleotide_Protein_Element) obj;
		if (country == null) {
			if (other.country != null)
				return false;
		} else if (!country.equals(other.country))
			return false;
		if (FullLengthSequence == null) {
			if (other.FullLengthSequence != null)
				return false;
		} else if (!FullLengthSequence.equals(other.FullLengthSequence))
			return false;
		if (GenbankAccessionNumber == null) {
			if (other.GenbankAccessionNumber != null)
				return false;
		} else if (!GenbankAccessionNumber.equals(other.GenbankAccessionNumber))
			return false;
		if (!GenomeSequenceNumber.equals(other.GenomeSequenceNumber))
			return false;
		if (Host == null) {
			if (other.Host != null)
				return false;
		} else if (!Host.equals(other.Host))
			return false;
		if (HostAge == null) {
			if (other.HostAge != null)
				return false;
		} else if (!HostAge.equals(other.HostAge))
			return false;
		if (HostGender == null) {
			if (other.HostGender != null)
				return false;
		} else if (!HostGender.equals(other.HostGender))
			return false;
		if (SequenceLength != other.SequenceLength)
			return false;
		if (SubType == null) {
			if (other.SubType != null)
				return false;
		} else if (!SubType.equals(other.SubType))
			return false;
		if (VirusName == null) {
			if (other.VirusName != null)
				return false;
		} else if (!VirusName.equals(other.VirusName))
			return false;
		if (YearFound != other.YearFound)
			return false;
		return true;
	}

	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("GenbankAccessionNumber=" + GenbankAccessionNumber);
		sb.append("\nHost=" + Host);
		sb.append("\nSubType=" + SubType);
		sb.append("\ncountry=" + country);
		sb.append("\nGenomeSequenceNumber=" + GenomeSequenceNumber);
		sb.append("\nYearFound=" + YearFound);
		sb.append("\nSequenceLength=" + SequenceLength);
		sb.append("\nVirusName=" + VirusName);
		sb.append("\nHostAge=" + HostAge);
		sb.append("\nHostGender=" + HostGender);
		return sb.toString();
	}


	String GenbankAccessionNumber;
	String Host;
	String SubType;
	String country;

	private String GenomeSequenceNumber;
	private int YearFound;
	private int SequenceLength;
	private String VirusName;
	private String HostAge;
	private String HostGender;
	private Boolean FullLengthSequence;
}
