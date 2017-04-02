package com.twitterbeam.app;

import java.util.Arrays;
import java.util.List;

import org.apache.avro.reflect.AvroAlias;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.BigEndianLongCoder;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
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
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;

import avro.shaded.com.google.common.collect.ImmutableList;
import avro.shaded.com.google.common.collect.ImmutableMap;

/**
 * Hello world!
 *
 */

public class BeamTwitterPipeline {
	public static Driver driver;
	public static Session session;

	static {
		driver = GraphDatabase.driver("bolt://localhost:7687", AuthTokens.basic("neo4j", "123456"));
		session = driver.session();
	}

	public static void main(String[] args) {
		TwitterBeamOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
				.as(TwitterBeamOptions.class);
		System.out.println("run");
		Pipeline pipeline = Pipeline.create(options);
		List<String> topics = ImmutableList.of("twitterbeam");
		KafkaIO.Read<String, String> kafkareader = KafkaIO.<String, String>read().withBootstrapServers("localhost:9092")
				.withTopics(topics).withKeyCoder(StringUtf8Coder.of()).withValueCoder(StringUtf8Coder.of());

		pipeline.apply(kafkareader).apply("ParseString", ParDo.of(new ParseString()))
		.apply("SaveNeo4j", ParDo.of(new SaveNeo4j()));;

		pipeline.run().waitUntilFinish();
		session.close();
		driver.close();
	}

	public static interface TwitterBeamOptions extends PipelineOptions {

	}

	static class ParseString extends DoFn<KafkaRecord<String, String>, UserHashtag> {
		@ProcessElement
		public void processElement(ProcessContext c) {
			KV<String, String> el =c.element().getKV();
			System.out.println("Beam Twitter " + el.getKey() + " " + el.getValue());
			UserHashtag uh =new UserHashtag(el.getKey(),el.getValue());
			if(!Strings.isNullOrEmpty(uh.username) && !Strings.isNullOrEmpty(uh.hashtag))
			c.output(uh);
		}		
	}
	static class SaveNeo4j extends DoFn<UserHashtag, String> {
		@ProcessElement
		public void processElement(ProcessContext c) {
			System.out.println("SaveNeo4j Twitter " + c.element().username + " " + c.element().hashtag);

			session.run("MERGE (u:User {username: {username}}) "
					  + "MERGE (t:HashTag {title: {hashtag}}) "
					  + "MERGE  (u)-[r:TALK_ABOUT]->(t) "
					  + "On Create set r.rank=1 "
					  + "On Match  set r.rank=r.rank+1 ",
					Values.parameters("username", c.element().username,"hashtag", c.element().hashtag));
		}		
	}
	
	@DefaultCoder(AvroCoder.class)
	public static class UserHashtag{	
		public String username;
		public String hashtag;
		public UserHashtag(){}
		public UserHashtag(String username, String hashtag) {
			super();
			this.username = username;
			this.hashtag = hashtag;
		}

	}
}
