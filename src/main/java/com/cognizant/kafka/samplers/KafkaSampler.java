package com.cognizant.kafka.samplers;


import com.cognizant.kafka.producer.KafkaBinaryProducer;
import org.apache.jmeter.config.Arguments;
import org.apache.jmeter.protocol.java.sampler.AbstractJavaSamplerClient;
import org.apache.jmeter.protocol.java.sampler.JavaSamplerContext;
import org.apache.jmeter.samplers.SampleResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

public class KafkaSampler extends AbstractJavaSamplerClient implements Serializable {

    private static final String KAFKA_BROKER = "KafkaBroker";
    private static final String KAFKA_TOPIC = "KafkaTopic";
    private static final String INPUT_FILE = "InputFile";
//    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaSampler.class);
    private static final KafkaBinaryProducer producer = new KafkaBinaryProducer();

    @Override
    public Arguments getDefaultParameters() {

        Arguments defaultParameters = new Arguments();
        defaultParameters.addArgument(KAFKA_BROKER,"Broker Name");
        defaultParameters.addArgument(KAFKA_TOPIC,"Topic Name");
        defaultParameters.addArgument(INPUT_FILE,"Text Message");
        return defaultParameters;
    }

    @Override
    public SampleResult runTest(JavaSamplerContext javaSamplerContext) {

        String broker = javaSamplerContext.getParameter(KAFKA_BROKER);
        String topicName = javaSamplerContext.getParameter(KAFKA_TOPIC);
        String file = javaSamplerContext.getParameter(INPUT_FILE);

        SampleResult sampleResult = new SampleResult();
        sampleResult.sampleStart();
        try {

            String responseData = producer.sendMessage(broker, topicName, file);
            sampleResult.sampleEnd();;
            sampleResult.setSuccessful(Boolean.TRUE);
            sampleResult.setResponseCode("200");
//            sampleResult.setRequestHeaders();
            sampleResult.setResponseCodeOK();
            sampleResult.setResponseMessage(responseData);
            sampleResult.setResponseData(responseData, "UTF-8");
        } catch (Exception e) {

//            LOGGER.error("Request was not successfully processed",e);
            sampleResult.sampleEnd();
            sampleResult.setResponseMessage(e.getMessage());
            sampleResult.setSuccessful(Boolean.FALSE);
        }
        return sampleResult;
    }
}