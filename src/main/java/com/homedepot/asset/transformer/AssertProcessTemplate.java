package com.homedepot.asset.transformer;

import lombok.extern.slf4j.Slf4j;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.homedepot.asset.transformer.fn.ExtractDataSetFn;
import com.homedepot.asset.transformer.fn.FeedOptions;


/**
 * Created by SXB8999.
 */
@Component
@Slf4j
public class AssertProcessTemplate{
	
	//@Autowired
	//BigQueryDataSetFn extractDataSetFn;
	
	@Autowired
	ExtractDataSetFn extractDataSetFn;
	
	
	public void run(String[] args) throws Exception {

		// Start by defining the options for the pipeline.
		FeedOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(FeedOptions.class);	
		// Then create the pipeline.
		Pipeline p = Pipeline.create(options);	
		
		//apply pipeline for Transform
		PCollection<String> output= p.apply(Create.of("Begin-Pipeine")).apply("Extract DataSet",extractDataSetFn) ;		
		
		final String header="itemid,guid,nm,val";
		output.apply("Write Result",TextIO.Write.to(options.getOutputFile())				
				.withHeader(header)
				.withSuffix(".csv"));	
		log.info("Dataflow -completed");
		
		// Run the pipeline.
		p.run();
		
		
	}

}
