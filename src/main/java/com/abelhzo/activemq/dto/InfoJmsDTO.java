package com.abelhzo.activemq.dto;

import java.io.Serializable;
import java.util.Date;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlSchemaType;

@XmlRootElement(name = "InfoJmsDTO")
@XmlAccessorType(XmlAccessType.FIELD)
public class InfoJmsDTO implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 6986783320480049295L;
	@XmlElement(name = "key", required = true)
	private Integer key;
	@XmlElement(name = "name", required = true)
	private String name;
	@XmlElement(name = "date", required = true)
	@XmlSchemaType(name="dateTime")
	private Date date;
	@XmlElement(name = "typeJms", required = true)
	private String typeJms;

	public Integer getKey() {
		return key;
	}

	public void setKey(Integer key) {
		this.key = key;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public Date getDate() {
		return date;
	}

	public void setDate(Date date) {
		this.date = date;
	}

	public String getTypeJms() {
		return typeJms;
	}

	public void setTypeJms(String typeJms) {
		this.typeJms = typeJms;
	}

}
