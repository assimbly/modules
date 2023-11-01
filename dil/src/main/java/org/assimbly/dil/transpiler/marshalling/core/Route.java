package org.assimbly.dil.transpiler.marshalling.core;

import org.apache.commons.configuration2.XMLConfiguration;
import org.apache.commons.lang3.StringUtils;
import org.assimbly.dil.transpiler.XMLFileConfiguration;
import org.assimbly.docconverter.DocConverter;
import org.assimbly.util.IntegrationUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Node;

import java.util.TreeMap;

public class Route {

    final static Logger log = LoggerFactory.getLogger(Route.class);
    private TreeMap<String, String> properties;
    private XMLConfiguration conf;

    public Route(TreeMap<String, String> properties, XMLConfiguration conf) {
        this.properties = properties;
        this.conf = conf;
    }

    public TreeMap<String, String> setRoute(String type, String flowId, String stepId, String routeId) throws Exception {

        String route = createRoute(flowId, routeId);

        route = createDataFormat(route);

        properties.put(type + "." + stepId + ".route.id", flowId + "-" + routeId);
        properties.put(type + "." + stepId + ".route", route);

        return properties;
    }

    private String createRoute(String flowId, String routeId) throws Exception {

        Node node = IntegrationUtil.getNode(conf,"/dil/core/routes/route[@id='" + routeId + "']");

        String routeAsString = DocConverter.convertNodeToString(node);

        if(routeAsString.contains("yamldsl")){
            routeAsString = StringUtils.substringBetween(routeAsString,"<yamldsl xmlns=\"http://camel.apache.org/schema/spring\">","</yamldsl>");
            routeAsString = StringUtils.replace(routeAsString,"id: " + routeId,"id: " + flowId + "-" + routeId);
        }else{
            routeAsString = StringUtils.replace(routeAsString,"id=\"" + routeId +"\"" ,"id=\"" + flowId + "-" + routeId +"\"");
        }

        return routeAsString;

    }

    private String createDataFormat(String route) throws Exception {

        String dataFormatAsString = null;
        if (route.contains("<customDataFormat ref=\"csv")){

            String ref = StringUtils.substringBetween(route, "<customDataFormat ref=\"", "\"/>");

            Node node = IntegrationUtil.getNode(conf,"/dil/core/routeConfigurations/routeConfiguration/dataFormats/csv[@id='" + ref +"']");

            dataFormatAsString = DocConverter.convertNodeToString(node);
            if(dataFormatAsString!=null) {
                route = route.replaceAll("<customDataFormat ref=(.*)", dataFormatAsString);
            }else{
                log.warn("Route:\n\n" + route + "\n\n Contains custom csv dataformat, but csv dataFormat is null");
            }
        }

        return route;

    }

}