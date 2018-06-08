package org.assimbly.connector.configuration;

import java.io.File;
import java.io.InputStream;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathFactory;

import org.apache.commons.configuration2.XMLConfiguration;
import org.apache.commons.configuration2.builder.BasicConfigurationBuilder;
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder;
import org.apache.commons.configuration2.builder.fluent.Parameters;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.io.FileHandler;
import org.apache.commons.configuration2.tree.xpath.XPathExpressionEngine;
import org.assimbly.connector.connect.util.ConnectorUtil;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.ErrorHandler;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;

public class XMLFileConfiguration {
	
	private TreeMap<String, String> properties;
	private List<TreeMap<String, String>> propertiesList;

	private String flowId;
	
	private XMLConfiguration conf;

	private String serviceId;
	private String serviceXPath;
	private String connectorXPath;
	
	private String uri;
	private String options;
	private String headerId;
	private Element rootElement;
	private Document doc;
	private Element flows;
	private Element services;
	private Element headers;
	private Element flow;
	private String xmlFlowConfiguration;

	private List<String> servicesList;
    private List<String> headersList;
	private Element connector;

	public List<TreeMap<String, String>> getConfiguration(String connectorId, String xml) throws Exception {
		
		propertiesList = new ArrayList<>();
		Document doc = ConnectorUtil.convertStringToDoc(xml);

		List<String> flowIds = getFlowIds(connectorId,doc); 
 		
  	   for(String flowId : flowIds){
  		 
	  		TreeMap<String, String> flowConfiguration = getFlowConfiguration(flowId, xml);
	  		 
	  		if(flowConfiguration!=null) {
	  	  		 propertiesList.add(flowConfiguration);
	  		}
  	   }
  	   
	   return propertiesList;
	
	}

	public List<TreeMap<String, String>> getConfiguration(String connectorId, URI uri) throws Exception {

		propertiesList = new ArrayList<>();
		Document doc = ConnectorUtil.convertUriToDoc(uri);

		List<String> flowIds = getFlowIds(connectorId,doc); 
 		
  	   for(String flowId : flowIds){
  		   TreeMap<String, String> flowConfiguration = getFlowConfiguration(flowId, uri);
  		 
  		if(flowConfiguration!=null) {
  	  		 propertiesList.add(flowConfiguration);
  		}
  	   }
  	   
	   return propertiesList;
	
	}
	
	public TreeMap<String, String> getFlowConfiguration(String flowId, String xml) throws Exception {

	   this.flowId = flowId;

	   DocumentBuilder docBuilder = setDocumentBuilder("connector.xsd");

	   conf = new BasicConfigurationBuilder<>(XMLConfiguration.class).configure(new Parameters().xml()
	           .setFileName("connector.xml")
	           .setDocumentBuilder(docBuilder)
	           .setSchemaValidation(true)	           
	           .setExpressionEngine(new XPathExpressionEngine())			   
			   ).getConfiguration();

	   FileHandler fh = new FileHandler(conf);
	   fh.load(ConnectorUtil.convertStringToStream(xml));

  	   setProperties();

  	   ConnectorUtil.printTreemap(properties);
  	 
	   return properties;
	   
	}
	
	public TreeMap<String, String> getFlowConfiguration(String flowId, URI uri) throws Exception {

	   this.flowId = flowId;

	   String scheme = uri.getScheme();
	   //load uri to configuration 
	   Parameters params = new Parameters();
	   
	   DocumentBuilder docBuilder = setDocumentBuilder("connector.xsd");	   
	   
		if(scheme.startsWith("sonicfs")) {

			URL Url = uri.toURL();

	        InputStream is = Url.openStream();        	        

	        conf = new BasicConfigurationBuilder<>(XMLConfiguration.class).configure(params.xml()).getConfiguration();
	        FileHandler fh = new FileHandler(conf);
	        fh.load(is);
			
		}else if (scheme.startsWith("file")) {
			
			   File xml = new File(uri.getRawPath());

			   FileBasedConfigurationBuilder<XMLConfiguration> builder =
			       new FileBasedConfigurationBuilder<XMLConfiguration>(XMLConfiguration.class)
			       .configure(params.xml()
			           .setFileName("connector.xml")
			           .setFile(xml)
			           .setDocumentBuilder(docBuilder)
			           .setSchemaValidation(true)
			           .setExpressionEngine(new XPathExpressionEngine())
			        );

			   // This will throw a ConfigurationException if the XML document does not
			   // conform to its Schema.
			   conf = builder.getConfiguration();
			   
		}else if (scheme.startsWith("http")) {
			
			URL Url = uri.toURL();
			
			FileBasedConfigurationBuilder<XMLConfiguration> builder =
			       new FileBasedConfigurationBuilder<XMLConfiguration>(XMLConfiguration.class)
			       .configure(params.xml()
			    	   .setURL(Url)
			           .setFileName("connector.xml")
			           .setDocumentBuilder(docBuilder)
			           .setSchemaValidation(true)
			           .setExpressionEngine(new XPathExpressionEngine())
			        );

			   // This will throw a ConfigurationException if the XML document does not
			   // conform to its Schema.
			   conf = builder.getConfiguration();				
		}else {
    		throw new Exception("URI scheme for " + uri.getRawPath() + " is not supported");        		        	
		}
		
	   setProperties();
	   
	   return properties;
	}

	public String createConfiguration(String connectorId, List<TreeMap<String, String>> configurations) throws Exception {

		if(configurations == null || configurations.isEmpty()) {
			return "Error: Empty configuration (no configuration is set/running)";
		}else {
		    DocumentBuilderFactory docFactory = DocumentBuilderFactory.newInstance();
		    DocumentBuilder docBuilder = docFactory.newDocumentBuilder();
		    doc = docBuilder.newDocument();
			
		    setGeneralProperties(connectorId);

			for (TreeMap<String, String> configuration : configurations) {
			    setFlowFromConfiguration(configuration);
			}

		    String xmlConfiguration = ConnectorUtil.convertDocToString(doc);
		    
			return xmlConfiguration;
			
		}

	}
	

	public String createFlowConfiguration(TreeMap<String, String> configuration) throws Exception {

		if(configuration == null || configuration.isEmpty()) {
			return "Error: Empty configuration (no configuration is set/running)";
		}
		
	    DocumentBuilderFactory docFactory = DocumentBuilderFactory.newInstance();
	    DocumentBuilder docBuilder = docFactory.newDocumentBuilder();
	    doc = docBuilder.newDocument();
		
	    setGeneralProperties("live");
	    
	    setFlowFromConfiguration(configuration);
	    
	    if(doc!=null) {
		    xmlFlowConfiguration = ConnectorUtil.convertDocToString(doc);
	    }else {
	    	xmlFlowConfiguration = "Error: Can't create configuration";
	    }
	    
		return xmlFlowConfiguration;
	}
	

	//--> private get/set methods
	
	private void setProperties() throws Exception{
		
		   //create Treemap from configuration
		   properties = new TreeMap<String, String>();

		   //set general properties
		   getGeneralPropertiesFromXMLFile();
		   
		   //set uri properties
		   String[] types = {"from", "to", "error"};

		   for(String type : types){
	  		   getURIfromXMLFile(type);
	  	   }
		   
		   	if(properties.get("from.uri") != null){
				properties.put("flow.type","default");
			}else{
				properties.put("flow.type", "none");
			}

			if(properties.get("to.uri") == null  || properties.get("to.uri").startsWith("wastebin")){
				properties.put("to.uri","mock:wastebin");		
			}
		
	}

	private void getGeneralPropertiesFromXMLFile() throws Exception{

		Document doc = conf.getDocument();
		   
	    XPath xPath = XPathFactory.newInstance().newXPath();
	    flowId = xPath.evaluate("//flows/flow[id='" + flowId + "']/id",doc);
		
		if(flowId==null || flowId.isEmpty()) {throw new ConfigurationException("Id (flow) doesn't exists in XML Configuration");}

		connectorXPath = "connectors/connector/flows/flow[id='" + flowId + "']";

	    String[] connectorProporties = conf.getStringArray(connectorXPath);
		
		if(connectorProporties.length > 0){
	  	   for(String connectorProperty : connectorProporties){
	  		   properties.put(connectorProperty.substring(connectorXPath.length() + 1), conf.getString(connectorProperty));
    	   }
		}
		
	   //set up defaults settings if null -->
		if(flowId == null){    			
			flowId = "flow" + System.currentTimeMillis();					
		}

		properties.put("id",flowId);	

		properties.put("header.contenttype", "text/xml;charset=UTF-8");
		
	}
	
	private void getURIfromXMLFile(String type) throws Exception {
		
		String componentsXpath = "//flows/flow[id='" + flowId + "']/" + type + "/uri";
	   
	    String[] components = conf.getStringArray(componentsXpath);
	    
	    String touri = "";
	    
	   int index = 1;		   

	   for(String component : components){

  		   options = "";	

		   List<String> optionProperties = ConnectorUtil.getXMLParameters(conf, "connectors/connector/flows/flow[id='" + flowId + "'][" + index + "]/" + type + "/options");
		   
	  	   for(String optionProperty : optionProperties){
			   options += optionProperty.split("options.")[1] + "=" + conf.getProperty(optionProperty) + "&";
	  	   }
	  	   
	  	   if(options.isEmpty()){
	  		 uri = component;
	  	   }else{
	  		 options = options.substring(0,options.length() -1);
	  		 uri = component + "?" + options;  
	  	   }	  	   
	  	   
		   getServiceFromXMLFile(type, index);
		   getHeaderFromXMLFile(type, index);
  		 
		   if(type.equals("from")||type.equals("error")) {
		  	   properties.put(type + ".uri", uri);
			   break;
		   }else {
			   if(touri.isEmpty()) {
				   touri = uri;
			   }else {
				   touri = touri + "," + uri;
			   }
			   
		  	   properties.put(type + ".uri", touri);
			   index++;   
		   }			   
  	   }	  	   
	}

	private void getServiceFromXMLFile(String type, int index) throws ConfigurationException {
		
	    serviceId = conf.getString("connector/flows/flow[id='" + flowId + "'][" + index + "]/" + type + "/service_id");
	    
	    if(serviceId == null){return;};

	    serviceXPath = "connector/services/service[id='" + serviceId + "']";
		List<String> serviceProporties = ConnectorUtil.getXMLParameters(conf, serviceXPath);
		
		if(!serviceProporties.isEmpty()){
			
			for(String serviceProperty : serviceProporties){
	  		   properties.put(type + ".service." + serviceProperty.substring(serviceXPath.length() + 1), conf.getString(serviceProperty));
    	   }
		}
		
	}
	
	private void getHeaderFromXMLFile(String type, int index) throws ConfigurationException {
	   	
	    headerId = conf.getString("connector/flows/flow[id='" + flowId + "'][\" + index + \"]/" + type + "/header_id");

	    if(headerId == null){return;};
	    
	    serviceXPath = "connector/headers/header[id='" + headerId + "']";
		List<String> headerProporties = ConnectorUtil.getXMLParameters(conf, serviceXPath);
		
		if(!headerProporties.isEmpty()){
	  	   for(String headerProperty : headerProporties){
	  		   String headerType = conf.getString(headerProperty + "/@type");
	  		   if(headerType!=null){
		  		   properties.put(type + ".header." + headerType + "." + headerProperty.substring(serviceXPath.length() + 1), conf.getString(headerProperty));
	  		   }
    	   }
		}
	}

    private static List<String> getFlowIds(String connectorId, Document doc)  throws Exception {

        // Create XPath object
        XPathFactory xpathFactory = XPathFactory.newInstance();
        XPath xpath = xpathFactory.newXPath();
        XPathExpression expr = xpath.compile("/connectors/connector[id=" + connectorId +"]/flows/flow/id/text()");
    	
        // Create list of Ids
    	List<String> list = new ArrayList<>();
        NodeList nodes = (NodeList) expr.evaluate(doc, XPathConstants.NODESET);
        for (int i = 0; i < nodes.getLength(); i++) {
            list.add(nodes.item(i).getNodeValue());
        }
        
        return list;
    }

	
	
	private void setGeneralProperties(String connectorId) {
		
	    rootElement = doc.createElement("connectors");
	    doc.appendChild(rootElement);

	    connector = doc.createElement("connector");
	    rootElement.appendChild(connector);
	    
	    Element id = doc.createElement("id");
	    id.appendChild(doc.createTextNode(connectorId));
	    connector.appendChild(id);
	    
	    flows = doc.createElement("flows");
	    services = doc.createElement("services");
	    headers = doc.createElement("headers");
	    
	    connector.appendChild(flows);
	    connector.appendChild(services);
	    connector.appendChild(headers);
	    
	    //List to ensure no double entries
	    servicesList = new ArrayList<String>();
	    headersList = new ArrayList<String>();   
		
	}
	
	
	private void setFlowFromConfiguration(TreeMap<String, String> configuration) throws Exception {

	    flow = doc.createElement("flow");
	    flows.appendChild(flow);
	    
	    //set id
	    String flowId = configuration.get("id");	    
	    Element id = doc.createElement("id");
	    id.appendChild(doc.createTextNode(flowId));
	    flow.appendChild(id);

	    //set id
	    String flowType = configuration.get("flow.type");	    
	    Element flowTypeNode = doc.createElement("type");
	    flowTypeNode.appendChild(doc.createTextNode(flowType));
	    flow.appendChild(flowTypeNode);	    
	    
	    //set endpoints
	    setFlowEndpoint("from",configuration);
	    setFlowEndpoint("to",configuration);
	    setFlowEndpoint("error",configuration);
	    
	}
	
	private void setFlowEndpoint(String type, TreeMap<String, String> configuration) throws Exception {

		String confUriList = configuration.get(type + ".uri");
		String confServiceId = configuration.get(type + ".service.id");
		String confHeaderId = configuration.get(type + ".header.id");

	    Element endpoint = doc.createElement(type);
	    Element uri = doc.createElement("uri");
	    Element options = doc.createElement("options");
	    Element serviceid = doc.createElement("service_id");
	    Element headerid = doc.createElement("header_id");
	    
		if(confUriList!=null) {
			
			String[] confUris = confUriList.split(",");
			
			for(String confUri : confUris) {

				flow.appendChild(endpoint);

				String[] confUriSplitted = confUri.split("\\?");
				
				if(confUriSplitted.length<=1) {
					if(confUri.startsWith("sonicmq")) {
						confUri = confUri.replaceFirst("sonicmq.*:", "sonicmq:");
					}
				    uri.setTextContent(confUri);	
					endpoint.appendChild(uri);
				}else {
					if(confUriSplitted[0].startsWith("sonicmq")) {
						confUriSplitted[0] = confUriSplitted[0].replaceFirst("sonicmq.*:", "sonicmq:");
					}
				    uri.setTextContent(confUriSplitted[0]);
				    endpoint.appendChild(uri);
				    endpoint.appendChild(options);
				    
				    String[] confOptions = confUriSplitted[1].split("&");
				    
				    for(String confOption : confOptions) {
				    	String[] confOptionSplitted = confOption.split("=");

				    	if(confOptionSplitted.length>1){
				    		Element option = doc.createElement(confOptionSplitted[0]);
						    option.setTextContent(confOptionSplitted[1]);	
				    		options.appendChild(option);
				    	}
				    }
				}
				
			    if(confServiceId!=null) {
				    serviceid.setTextContent(confServiceId);
			    	endpoint.appendChild(serviceid);
				    setServiceFromConfiguration(confServiceId, type, configuration);
				}

			    if(confHeaderId!=null) {
				    endpoint.appendChild(headerid);
				    headerid.setTextContent(confHeaderId);
				    setHeaderFromConfiguration(confHeaderId, type, configuration);
				}
			}
		}
	}

	private void setServiceFromConfiguration(String serviceid, String type, TreeMap<String, String> configuration) throws Exception {

		if(!servicesList.contains(serviceid)) {
			servicesList.add(serviceid);

		    Element service = doc.createElement("service");
		    services.appendChild(service);
	
			for(Map.Entry<String,String> entry : configuration.entrySet()) {
				  String key = entry.getKey();
				  String parameterValue = entry.getValue();
				  
				  if(key.startsWith(type + ".service") && parameterValue!=null) {
					  String parameterName = key.substring(key.lastIndexOf(".service.") + 9);				  
					  Element serviceParameter = doc.createElement(parameterName);
					  serviceParameter.setTextContent(parameterValue);
					  service.appendChild(serviceParameter);
				  }
			 }
		}
	}
	
	private void setHeaderFromConfiguration(String headerid, String type, TreeMap<String, String> configuration) throws Exception {

		if(!headersList.contains(headerid)) {
			headersList.add(headerid);

		    Element header = doc.createElement("header");
		    Element id = doc.createElement("id");
		    
		    headers.appendChild(header);
		    id.appendChild(doc.createTextNode(headerid));
		    header.appendChild(id);
	
			for(Map.Entry<String,String> entry : configuration.entrySet()) {
				String key = entry.getKey();
				String parameterValue = entry.getValue();
				  
				if(key.startsWith(type + ".header") && parameterValue!=null) {
					  String parameterName = key.substring(key.lastIndexOf(".header.") + 8);			  
					  Element headerParameter = doc.createElement(parameterName);
					  headerParameter.setTextContent(parameterValue);
					  header.appendChild(headerParameter);
			    }
			}		
		}		
	}
	
	private DocumentBuilder setDocumentBuilder(String schemaFilename) throws SAXException, ParserConfigurationException {
		
		   URL schemaUrl = this.getClass().getResource("/" + schemaFilename);
	       Schema schema = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI).newSchema(schemaUrl);
	    		   
	       DocumentBuilderFactory docBuilderFactory = DocumentBuilderFactory.newInstance();
	       docBuilderFactory.setSchema(schema);
	  
	        DocumentBuilder docBuilder = docBuilderFactory.newDocumentBuilder();
	        //if you want an exception to be thrown when there is invalid xml document,
	        //you need to set your own ErrorHandler because the default
	        //behavior is to just print an error message.
	        docBuilder.setErrorHandler(new ErrorHandler() {
	           @Override
	           public void warning(SAXParseException exception) throws SAXException {
	               throw exception;
	           }

	           @Override
	           public void error(SAXParseException exception) throws SAXException {
	               throw exception;
	           }

	           @Override
	           public void fatalError(SAXParseException exception)  throws SAXException {
	               throw exception;
	           }  
	       });
	        
	      return docBuilder;  
	}	
	
}