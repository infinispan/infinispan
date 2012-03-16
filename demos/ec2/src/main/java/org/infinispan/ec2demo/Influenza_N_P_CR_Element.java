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
import java.util.HashMap;
import java.util.Map;

/**
 * @author noconnor@redhat.com
 * 
 */
public class Influenza_N_P_CR_Element implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = -8703474569822388524L;
	private String ganNucleoid;
	private Map<String, String> proteinMap;

	/**
	 * @param proteinMap
	 */
	public Influenza_N_P_CR_Element() {
		super();
		this.proteinMap = new HashMap<String, String>();
	}

	public String getGanNucleoid() {
		return ganNucleoid;
	}

	public void setGanNucleoid(String ganNucleoid) {
		this.ganNucleoid = ganNucleoid;
	}

	public void setProtein_Data(String proteinGAN, String proteinCR) {
		if ((proteinGAN==null)||(proteinGAN.isEmpty()))
			return;
		
		if ((proteinCR==null)||(proteinCR.isEmpty()))
			return;
		
		proteinMap.put(proteinGAN, proteinCR);
	}
	
	public Map<String,String> getProtein_Data(){
		return this.proteinMap;
	}
	
	public String toString(){
		return "GAN="+this.ganNucleoid+" protein count="+this.proteinMap.size();
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((ganNucleoid == null) ? 0 : ganNucleoid.hashCode());
		result = prime * result + ((proteinMap == null) ? 0 : proteinMap.hashCode());
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
		Influenza_N_P_CR_Element other = (Influenza_N_P_CR_Element) obj;
		if (ganNucleoid == null) {
			if (other.ganNucleoid != null)
				return false;
		} else if (!ganNucleoid.equals(other.ganNucleoid))
			return false;
		if (proteinMap == null) {
			if (other.proteinMap != null)
				return false;
		} else if (!proteinMap.equals(other.proteinMap))
			return false;
		return true;
	}
}
