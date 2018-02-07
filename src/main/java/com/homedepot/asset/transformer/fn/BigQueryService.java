package com.homedepot.asset.transformer.fn;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;

import org.springframework.stereotype.Service;

import com.google.api.client.json.GenericJson;
import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.Bigquery.Jobs.GetQueryResults;
import com.google.api.services.bigquery.BigqueryRequest;
import com.google.api.services.bigquery.model.GetQueryResultsResponse;
import com.google.api.services.bigquery.model.QueryRequest;
import com.google.api.services.bigquery.model.QueryResponse;
import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.dataflow.sdk.transforms.DoFn;

@Service

public class BigQueryService extends DoFn<String, List<TableRow>> {
	private static final long serialVersionUID = 3562881329022659608L;
	private static final String PROJECT_ID = "hd-www-dev";

	@Override
	public void processElement(
			DoFn<String, List<TableRow>>.ProcessContext context)
			throws Exception {
		FeedOptions options = context.getPipelineOptions()
				.as(FeedOptions.class);
		String tableName = options.getTableName().get();
		context.output(getBigQueryResult(tableName));
	}

	public List<TableRow> getBigQueryResult(String tableName)
			throws IOException, InterruptedException {

		List<List<TableRow>> bigList = new ArrayList<List<TableRow>>();
		// Start a Query Job
		String querySql = "SELECT itemid,guid,nm,val FROM hdcom."
				+ tableName
				+ " WHERE (guid = 'f6ef55f4-9f5b-46d9-869d-40b671d668c1'  /* thd online status */    and val = 'true'  /* available online */) "
				+ "or    (guid = 'b5dead77-95a2-44bf-a472-d64458d1b103'  /* selling sites */        and val = 'Interline'  /* Interline selling */) "
				+ "or    (guid = '7eb1e571-64d3-4800-85ff-7bae9f84e334'  /* ship from YOW/CHUB  */  and val = 'YOW'  /* Interline selling */) "
				+ "or    (guid = '6f8487bb-858f-4c1c-b17a-8c91f2b57ff5'  /* thd in market 401  */   and val = 'Y'  /* Interline selling */) "
		        + "LIMIT 1000";

		Iterator<GetQueryResultsResponse> pages = run(PROJECT_ID, querySql, 20);
		while (pages.hasNext()) {
			bigList.add(pages.next().getRows());
		}		
		List<TableRow> extendList= bigList.stream()
                .flatMap(List::stream)
                .collect(Collectors.toList());
				
		return extendList;
		

	}

	public static Iterator<GetQueryResultsResponse> run(String projectId,
			String queryString, long waitTime) throws IOException {
		Bigquery bigquery = GCPServiceAccount.getService();
		// Wait until query is done with 20 second timeout, at most 5 retries on error
		QueryResponse query = bigquery
				.jobs()
				.query(projectId,
						new QueryRequest().setTimeoutMs(waitTime).setQuery(
								queryString)).execute();

		// Make a request to get the results of the query (timeout is zero since job should be complete)

		GetQueryResults getRequest = bigquery.jobs().getQueryResults(
				query.getJobReference().getProjectId(),
				query.getJobReference().getJobId());

		return getPages(getRequest);
	}

	public static <T extends GenericJson> Iterator<T> getPages(
			BigqueryRequest<T> request_template) {

		class PageIterator implements Iterator<T> {

			BigqueryRequest<T> request;
			boolean hasNext = true;

			public PageIterator(BigqueryRequest<T> request_template) {
				this.request = request_template;
			}

			public boolean hasNext() {
				return hasNext;
			}

			public T next() {
				if (!hasNext) {
					throw new NoSuchElementException();
				}
				try {
					T response = request.execute();
					if (response.containsKey("pageToken")) {
						request = request.set("pageToken",
								response.get("pageToken"));
					} else {
						hasNext = false;
					}
					return response;
				} catch (IOException e) {
					e.printStackTrace();
					return null;
				}
			}

			public void remove() {
				this.next();
			}
		}

		return new PageIterator(request_template);
	}

}
