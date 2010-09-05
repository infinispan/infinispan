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
