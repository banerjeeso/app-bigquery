package com.homedepot.asset.transformer.fn;

import java.io.IOException;
import java.io.InputStream;

import lombok.extern.slf4j.Slf4j;

import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.stereotype.Service;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.util.Utils;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.BigqueryScopes;

@Service
@Slf4j
public class GCPServiceAccount {

	private static Bigquery service = null;
	private static Object service_lock = new Object();

	private static final HttpTransport TRANSPORT = new NetHttpTransport();
	private static final JsonFactory JSON_FACTORY = new JacksonFactory();

	public static Bigquery getService() throws IOException {
		if (service == null) {
			synchronized (service_lock) {
				if (service == null) {
					service = createAuthorizedClient();
				}
			}
		}
		return service;
	}

	public static Bigquery createAuthorizedClient() throws IOException {

		GoogleCredential credential = loadClientSecrets();
		return new Bigquery(TRANSPORT, JSON_FACTORY, credential);
	}

	/**
	 * Helper to load client ID/Secret from file.
	 * 
	 * @return a GoogleClientSecrets object based on a
	 *         hd-www-dev-41d65ce019e7.json
	 */
	private static GoogleCredential loadClientSecrets() {
		try {
			Resource resource = new ClassPathResource(
					"hd-www-dev-41d65ce019e7.json");
			InputStream inputStream = resource.getInputStream();

			GoogleCredential clientSecrets = GoogleCredential.fromStream(
					inputStream, Utils.getDefaultTransport(),
					Utils.getDefaultJsonFactory());

			if (clientSecrets.createScopedRequired()) {
				clientSecrets = clientSecrets
						.createScoped(BigqueryScopes.all());
			}
			return clientSecrets;
		} catch (Exception e) {
			log.info("Could not load client secrets file ");
			e.printStackTrace();
		}
		return null;
	}

}