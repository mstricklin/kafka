package com.ticongeo.fame.kafka;

import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

/**
 * @author mstricklin
 * @version 1.0
 */
// PeriodicTipEvent
public class NodeK implements Processor<String, String> {

	@SuppressWarnings("unused")
	private static final Logger CLASS_LOGGER = LoggerFactory.getLogger((new Throwable()).getStackTrace()[0].getClassName());

	@SuppressWarnings("unused")
	private static final String NEWLINE = System.getProperty("line.separator");

	@Override
	public void init(ProcessorContext context) {
		// TODO Auto-generated method stub
		CLASS_LOGGER.warn("init");

	}

	@Override
	public void process(String key,
	                    String value) {
		CLASS_LOGGER.info("key: {} value: {}", key, value);
	}

	@Override
	public void punctuate(long timestamp) { }

	@Override
	public void close() {
		// TODO Auto-generated method stub
		CLASS_LOGGER.warn("close");
	}
}