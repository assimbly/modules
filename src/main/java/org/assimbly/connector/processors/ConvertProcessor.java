package org.assimbly.connector.processors;

import java.io.File;
import java.io.InputStream;

import org.apache.camel.Exchange;
import org.apache.camel.Message;
import org.apache.camel.Processor;
import org.assimbly.docconverter.DocConverter;


public class ConvertProcessor implements Processor {
	
	private String convertedBody;
	
	public void process(Exchange exchange) throws Exception {
		  
		Message in = exchange.getIn();
		Object convertFormatObject = in.getHeader("ConvertFormat");
		Object convertTypeObject = in.getHeader("ConvertType");

		if(convertFormatObject!=null) {
			
			String convertFormat = convertFormatObject.toString();
			String body = in.getBody(String.class);
			
			switch(convertFormat) 
	        { 
	            case "XML2JSON": 
	            	convertedBody = DocConverter.convertXmlToJson(body); 
	                break; 
	            case "XML2YAML":
	            	convertedBody = body; //DocConverter.convertXmltoYaml(body);
	                break; 
	            case "XML2CSV": 
	            	convertedBody = body; //DocConverter.convertXmltoCsv(body);
	                break;	         
	            case "JSON2XML": 
	            	convertedBody = DocConverter.convertJsonToXml(body); 
	                break; 
	            case "JSON2YAML":
	            	convertedBody = DocConverter.convertJsonToYaml(body);
	                break; 
	            case "JSON2CSV": 
	            	convertedBody = DocConverter.convertJsonToCsv(body);
	                break;
	            case "YAML2XML": 
	    			convertedBody = DocConverter.convertYamlToXml(body);
	                break; 
	            case "YAML2JSON":
	    			convertedBody = DocConverter.convertYamlToJson(body);
	                break; 
	            case "YAML2CSV": 
	    			convertedBody = DocConverter.convertYamlToCsv(body);
	                break;	                
	            case "CSV2XML": 
	    			convertedBody = DocConverter.convertCsvToXml(body);
	                break; 
	            case "CSV2JSON":
	    			convertedBody = DocConverter.convertCsvToJson(body);
	                break; 
	            case "CSV2YAML": 
	    			convertedBody = DocConverter.convertCsvToYaml(body);
	                break;	                
	                
	        }

            in.setBody(convertedBody);

		}
		
		if(convertTypeObject!=null) {
			
			String convertTypeTo = convertTypeObject.toString();
			
			switch(convertTypeTo) 
	        { 
	            case "Body2Bytes": 
	                in.setBody(in.getBody(), byte[].class);
	                break; 
	            case "Body2String": 
	                in.setBody(in.getBody(), String.class);
	                break; 
	            case "Body2File": 
	                in.setBody(in.getBody(), File.class);
	                break;
	            case "Body2InputStream":
	                in.setBody(in.getBody(), InputStream.class);
	                break;
	        }
					
		}
		
	}	
	
}
