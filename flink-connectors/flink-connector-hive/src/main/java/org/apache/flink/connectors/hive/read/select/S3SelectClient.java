/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connectors.hive.read.select;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.SelectObjectContentEventVisitor;
import com.amazonaws.services.s3.model.SelectObjectContentRequest;
import com.amazonaws.services.s3.model.SelectObjectContentResult;
import org.apache.hadoop.conf.Configuration;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;

import static com.amazonaws.services.s3.model.SelectObjectContentEvent.EndEvent;
import static java.util.Objects.requireNonNull;
import static org.apache.flink.shaded.guava18.com.google.common.base.Strings.isNullOrEmpty;

/**
 * AmazonS3 Client for Flink.
 */
class S3SelectClient implements Closeable {

	private static final String S3_ACCESS_KEY = "fs.s3a.access.key";
	private static final String S3_SECRET_KEY = "fs.s3a.secret.key";
	private static final String S3_ENDPOINT = "fs.s3a.endpoint";

	private final AmazonS3 amazonS3;
	private boolean receivedEndEvent;
	private SelectObjectContentResult selectObjectContentResult;

	public S3SelectClient(Configuration configuration) {
		requireNonNull(configuration, "configuration is null");
		this.amazonS3 = getAmazonS3(configuration);
	}

	private AmazonS3 getAmazonS3(Configuration config) {
		String accessKey = config.get(S3_ACCESS_KEY);
		String secretKey = config.get(S3_SECRET_KEY);
		String endpoint = config.get(S3_ENDPOINT);

		if (isNullOrEmpty(accessKey) || isNullOrEmpty(secretKey) || isNullOrEmpty(endpoint)) {
			throw new RuntimeException("Config Missing, please check your AK / SK / EndPoint !");
		}

		AWSCredentials credentials = new BasicAWSCredentials(accessKey, secretKey);
		ClientConfiguration conf = new ClientConfiguration()
			.withMaxConnections(200)
			.withSocketTimeout(1000)
			.withMaxErrorRetry(3)
			.withUserAgentSuffix("flink-select");

		AmazonS3 amazonS3 = AmazonS3ClientBuilder.standard()
			.withCredentials(new AWSStaticCredentialsProvider(credentials))
			.withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(endpoint, null))
			.withClientConfiguration(conf)
			.withPathStyleAccessEnabled(false)
			.build();

		return amazonS3;
	}

	public InputStream getSelectRecordsInputStream(SelectObjectContentRequest selectObjectContentRequest) {
		if (selectObjectContentRequest == null) {
			throw new NullPointerException();
		}

		this.selectObjectContentResult = amazonS3.selectObjectContent(selectObjectContentRequest);
		return selectObjectContentResult.getPayload().getRecordsInputStream(
			new SelectObjectContentEventVisitor() {
				@Override
				public void visit(EndEvent endEvent) {
					receivedEndEvent = true;
				}
			});
	}

	@Override
	public void close() throws IOException {
		if (selectObjectContentResult != null) {
			selectObjectContentResult.close();
		}
	}

	/**
	 * If receivedEndEvent is true, means that the results is complete.
	 */
	public boolean isReceivedEndEvent() {
		return receivedEndEvent;
	}
}
