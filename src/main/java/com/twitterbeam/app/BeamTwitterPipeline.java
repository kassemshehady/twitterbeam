package com.twitterbeam.app;

import java.util.Arrays;
import java.util.List;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.BigEndianLongCoder;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.PubsubIO;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptions.DirectRunner;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.DoFn.ProcessContext;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;
import org.apache.beam.sdk.values.PCollection;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import avro.shaded.com.google.common.collect.ImmutableList;
import avro.shaded.com.google.common.collect.ImmutableMap;

/**
 * Hello world!
 *
 */

public class BeamTwitterPipeline {
	private static final Logger LOG = LoggerFactory.getLogger(BeamTwitterPipeline.class);

	public static void main(String[] args) {
		TwitterBeamOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
			      .as(TwitterBeamOptions.class);		
		System.out.println("run");
		Pipeline pipeline = Pipeline.create(options);
		List<String> topics = ImmutableList.of("beamtwitter");
		KafkaIO.Read<String, String> kafkareader = KafkaIO.<String, String>read()
				.withBootstrapServers("localhost:9092")
				.withTopics(topics)
				 .withKeyCoder(StringUtf8Coder.of())
			        .withValueCoder(StringUtf8Coder.of());

		pipeline.apply(kafkareader).apply("ParseString", ParDo.of(new ParseString()));
		
		pipeline.run().waitUntilFinish();
		
	}
	public static interface TwitterBeamOptions extends PipelineOptions {
		  
		}
	static class ParseString extends DoFn<KafkaRecord<String, String>, String> {
		@ProcessElement
		public void processElement(ProcessContext c) {
			System.out.println("Beam Twitter " + c.element().getKV().getValue());
			c.output(new String(c.element().getKV().getValue()));
		}
	}
}
