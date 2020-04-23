package com.example.xml_to_avro;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.xml.sax.SAXException;

import java.io.File;
import java.io.IOException;

@SpringBootApplication
public class XmlToAvroApplication {

	public static void main(String[] args) throws IOException, SAXException {
		SpringApplication.run(XmlToAvroApplication.class, args);

		File inputXmlFile = new File("input.xml");
		File outputAvroFile = new File("xml_to_avro.avro");
		File avroToXML = new File("avro_to_xml.xml");
		XmlToAvroConverter xmlToAvroConverter = new XmlToAvroConverter();
		xmlToAvroConverter.xmlToAvro( inputXmlFile,  outputAvroFile);
		xmlToAvroConverter.avroToXml( outputAvroFile,  avroToXML);
	}

}
