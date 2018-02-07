package com.homedepot.asset.transformer.fn;

import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.ValueProvider;

/**
 * Created by sxb8999.
 */
public interface FeedOptions extends DataflowPipelineOptions {
			   
	    @Description("Path of the file to write to")	   
	    ValueProvider<String> getOutputFile();
	    void setOutputFile(ValueProvider<String> value);
	    
		@Description("Path of the input Bucket to read feed file ")   
	 	ValueProvider<String> getTableName();	   
	    void setTableName(ValueProvider<String> value);	

}
