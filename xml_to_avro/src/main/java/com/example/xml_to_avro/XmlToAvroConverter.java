package com.example.xml_to_avro;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

@RestController
public class XmlToAvroConverter {



    @GetMapping("/xmlToAvro")
    public static void xmlToAvro(File inputXmlFile, File avroOutPutFile) throws IOException, SAXException {

        Schema.Parser parser = new Schema.Parser();
        InputStream stream =new FileInputStream("xml.avsc");
        Schema schema = parser.parse(stream);

        Document doc = parse(inputXmlFile);
        DatumWriter<GenericRecord> datumWriter = new SpecificDatumWriter<>(schema);
        try (DataFileWriter<GenericRecord> fileWriter = new DataFileWriter<>(datumWriter)) {
            fileWriter.create(schema, avroOutPutFile);
            fileWriter.append(wrapElement(doc.getDocumentElement(),schema));
        }

        System.out.println("XML to Avro Convertion completed......");
    }

    private static GenericData.Record wrapElement(Element el, Schema schema) {
        GenericData.Record record = new GenericData.Record(schema);
        record.put("Name", el.getElementsByTagName("Name").item(0).getTextContent());
        record.put("Id", Integer.valueOf(el.getElementsByTagName("Id").item(0).getTextContent()));
        return record;
    }

    private static Document parse(File file) throws IOException, SAXException {
        try {
            DocumentBuilder builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
            return builder.parse(file);
        } catch (ParserConfigurationException e) {
            throw new RuntimeException(e);
        }
    }


    public  void avroToXml(File avroFile, File xmlFile) throws IOException {
        Schema.Parser parser = new Schema.Parser();
        InputStream stream =new FileInputStream("xml.avsc");
        Schema schema = parser.parse(stream);
        DatumReader<GenericRecord> datumReader = new GenericDatumReader<>(schema);
        DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(avroFile, datumReader);
        GenericRecord record = dataFileReader.next();
        Document doc;
        try {
            doc = DocumentBuilderFactory.newInstance().newDocumentBuilder().newDocument();
        } catch (ParserConfigurationException e) {
            throw new RuntimeException(e);
        }
        Element employee = doc.createElement("Employee");
        doc.appendChild(employee);
        Element name=doc.createElement("Name");
        name.appendChild(doc.createTextNode(""+record.get("Name")));
        employee.appendChild(name);
        Element id=doc.createElement("Id");
        id.appendChild(doc.createTextNode(""+record.get("Id")));
        employee.appendChild(id);

        saveDocument(doc, xmlFile);

        System.out.println("Avro to XML Convertion completed......");
    }

    private static void saveDocument(Document doc, File file) {
        try {
            Transformer transformer = TransformerFactory.newInstance().newTransformer();
            transformer.transform(new DOMSource(doc), new StreamResult(file));
        } catch (TransformerException e) {
            throw new RuntimeException(e);
        }
    }

}
