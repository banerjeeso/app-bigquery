package com.homedepot.asset.transformer.fn;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.dataflow.sdk.io.BigQueryIO;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.PCollection;

@Component

public class ExtractDataSetFn extends
		PTransform<PCollection<String>, PCollection<String>> {

	private static final long serialVersionUID = 1204903774545788871L;
	@Autowired
	BigQueryService bqService;

	@Override
	public PCollection<String> apply(PCollection<String> input) {

		PCollection<List<TableRow>> tableResult = input.apply(ParDo.named(
				"BIG QUERY").of(bqService));

		
		PCollection<TableRow> attributesDataInfo = tableResult.apply("Format TableRow",ParDo
				.of(new ExtractTableRecordFn()));

		return attributesDataInfo.apply("Create Results Set",ParDo.of(new FormatResultsFn()));
	}

	static class ExtractTableRecordFn extends DoFn<List<TableRow>, TableRow> {
		private static final long serialVersionUID = 4783787393271367614L;

		@Override
		public void processElement(ProcessContext c) {
			List<TableRow> rows = c.element();
			rows.forEach(row -> c.output(row));
		}
	}

	static class FormatResultsFn extends DoFn<TableRow, String> {
		private static final long serialVersionUID = -3336277839332056989L;
		
		@Override
		public void processElement(ProcessContext c) throws Exception {
			TableRow row = c.element();
			StringBuilder b = new StringBuilder();
			row.getF().forEach(field -> {
				if(field!=null){
					b.append(field.getV() + ",");
				}else{
					b.append("NULL,");
				}
				
			});
			c.output(b.toString());
		}
	}

}
