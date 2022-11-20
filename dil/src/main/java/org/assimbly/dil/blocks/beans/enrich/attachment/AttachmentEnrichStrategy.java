package org.assimbly.dil.blocks.beans.enrich.attachment;

import org.apache.axiom.attachments.ByteArrayDataSource;
import org.apache.camel.AggregationStrategy;
import org.apache.camel.Exchange;
import org.apache.camel.Message;
import org.apache.camel.attachment.AttachmentMessage;
import org.apache.tika.io.IOUtils;
import org.assimbly.util.helper.MimeTypeHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.activation.DataHandler;
import java.io.IOException;
import java.io.InputStream;

public class AttachmentEnrichStrategy implements AggregationStrategy {

    protected Logger log = LoggerFactory.getLogger(getClass());

    @Override
    public Exchange aggregate(Exchange original, Exchange resource) {
        if (original == null) {
            throw new NullPointerException("Original exchange is null, cannot add resource as attachment.");
        }

        Message resourceMessage;


        // When the resource message is null, return the original exchange without adding any attachments.
        // In this case the main flow can keep going, but the error is logged to the log tab in Dovetail.
        try {
            resourceMessage = resource.getIn();
        } catch (NullPointerException e){
            NullPointerException exception = new NullPointerException("Dovetail could not get a response from your requested resource.");
            log.error(exception.getMessage());

            return original;
        }

        // Default attachment name == message id of incoming resource
        String attachmentName = resourceMessage.getMessageId();

        if (resourceMessage.getHeader(Exchange.FILE_NAME) != null) {
            attachmentName = resourceMessage.getHeader(Exchange.FILE_NAME, String.class);
        }

        InputStream body = resourceMessage.getBody(InputStream.class);

        String mimeType = MimeTypeHelper.detectMimeType(body).toString();

        if (resourceMessage.getHeader(Exchange.CONTENT_TYPE) != null) {
            mimeType = resourceMessage.getHeader(Exchange.CONTENT_TYPE, String.class);
        }

        DataHandler dataHandler = null;
        byte[] data = new byte[0];

        try {
            data = IOUtils.toByteArray(body);
        } catch (IOException e) { log.error(e.getMessage()); }

        ByteArrayDataSource byteArrayDataSource = new ByteArrayDataSource(data, mimeType);

        dataHandler = new DataHandler(byteArrayDataSource);

        log.info(String.format("Adding attachment '%s' with mime type: '%s'", attachmentName, mimeType));

        log.info("Attachment details");
        log.info(String.format("\tsize: %s", data.length));

        AttachmentMessage am = original.getMessage(AttachmentMessage.class);
        am.addAttachment(attachmentName, dataHandler);

        return original;
    }
}
