package com.ote.test.logback.appender;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.AppenderBase;
import ch.qos.logback.core.encoder.Encoder;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class RabbitMQAppender extends AppenderBase<ILoggingEvent> {

    @Setter
    private Encoder<ILoggingEvent> encoder;

    @Setter
    private String virtualHost;

    @Setter
    private String host;

    @Setter
    private String port;

    @Setter
    private String username;

    @Setter
    private String password;

    @Setter
    private String queue;

    private Connection connection;

    private Channel channel;

    @Override
    public void start() {

        try {
            boolean hasError = assertParameters();
            if (!hasError) {
                this.openConnection();
                super.start();
            }
        } catch (Exception e) {
            String message = "Error while starting RabbitMQAppender : " + e.getMessage();
            System.err.println(message + ". Enable debug mode on logback configuration to know more");
            addWarn(message, e);
        }
    }

    private boolean assertParameters() {

        boolean hasError = false;

        if (StringUtils.isEmpty(this.virtualHost)) {
            addWarn("VirtualHost might be missing");
        }

        if (StringUtils.isEmpty(this.host)) {
            addWarn("Host might be missing ('localhost' by default)");
        }

        if (StringUtils.isEmpty(this.port)) {
            addWarn("Port might be missing ('5672' by default)");
        } else if (!StringUtils.isNumeric(this.port)) {
            addWarn("Port must be numeric");
            hasError = true;
        }

        if (StringUtils.isEmpty(this.username)) {
            addWarn("Username might be missing ('guest' by default)");
        }

        if (StringUtils.isEmpty(this.password)) {
            addWarn("Password might be missing ('guest' by default)");
        }

        if (StringUtils.isEmpty(this.queue)) {
            addWarn("Queue must be set");
            hasError = true;
        }

        return hasError;
    }

    private void openConnection() throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();

        if (StringUtils.isNotEmpty(this.virtualHost)) {
            factory.setVirtualHost(this.virtualHost);
        }

        if (StringUtils.isNotEmpty(this.host)) {
            factory.setHost(this.host);
        }

        if (StringUtils.isNotEmpty(this.port)) {
            factory.setPort(Integer.parseInt(this.port));
        }

        if (StringUtils.isNotEmpty(this.username)) {
            factory.setUsername(this.username);
        }

        if (StringUtils.isNotEmpty(this.password)) {
            factory.setPassword(this.password);
        }

        this.connection = factory.newConnection();
        this.channel = connection.createChannel();
    }

    @Override
    protected void append(ILoggingEvent event) {
        try {
            this.channel.basicPublish("", this.queue, null, this.encoder.encode(event));
        } catch (Exception e) {
            String message = "Error while sending message to queue '" + this.queue + "' " + e.getMessage();
            System.err.println(message + ". Enable debug mode on logback configuration to know more");
            addWarn(message, e);
        }
    }

    @Override
    public void stop() {
        try {
            if (this.channel != null) {
                this.channel.close();
            }
            if (this.connection != null) {
                this.connection.close();
            }
        } catch (Exception e) {
            String message = "Error while stopping RabbitMQAppender : " + e.getMessage();
            System.err.println(message + ". Enable debug mode on logback configuration to know more");
            addWarn(message, e);
        }
    }
}
