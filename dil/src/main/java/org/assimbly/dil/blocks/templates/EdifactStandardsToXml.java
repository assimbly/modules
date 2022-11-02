package org.assimbly.dil.blocks.templates;

import org.apache.camel.builder.RouteBuilder;

public class EdifactStandardsToXml extends RouteBuilder {

     @Override
     public void configure() throws Exception {

         routeTemplate("edifactstandardstoxml-action")
                 .templateParameter("routeconfiguration_id","0")
                 .templateParameter("path")
                 .templateParameter("in")
                 .templateParameter("out")
                 .from("{{in}}")
                     .routeConfigurationId("{{routeconfiguration_id}}")
                     .to("edifact-standards:{{path}}")
                     .to("{{out}}");

         routeTemplate("edifactstandardstoxml-sink")
                 .templateParameter("routeconfiguration_id","0")
                 .templateOptionalParameter("path")
                 .templateParameter("in")
                 .from("{{in}}")
                     .routeConfigurationId("{{routeconfiguration_id}}")
                     .to("edifact:standards:{{path}}");
    }

}
