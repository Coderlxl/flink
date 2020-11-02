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

import com.amazonaws.services.s3.model.CSVInput;
import com.amazonaws.services.s3.model.CSVOutput;
import com.amazonaws.services.s3.model.CompressionType;
import com.amazonaws.services.s3.model.ExpressionType;
import com.amazonaws.services.s3.model.InputSerialization;
import com.amazonaws.services.s3.model.OutputSerialization;
import com.amazonaws.services.s3.model.SelectObjectContentRequest;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.util.LineReader;

import javax.annotation.concurrent.ThreadSafe;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

import static org.apache.flink.core.fs.Path.SEPARATOR;
import static org.apache.hadoop.hive.serde.serdeConstants.ESCAPE_CHAR;
import static org.apache.hadoop.hive.serde.serdeConstants.FIELD_DELIM;
import static org.apache.hadoop.hive.serde.serdeConstants.LINE_DELIM;
import static org.apache.hadoop.hive.serde.serdeConstants.QUOTE_CHAR;
import static org.apache.hadoop.hive.serde.serdeConstants.SERIALIZATION_FORMAT;

/**
 * Csv Reader with S3 Select API.
 */
@ThreadSafe
public class S3SelectCsvReader implements RecordReader<Writable, Writable> {

	private final long start;
	private final long end;
	private long pos;

	private boolean isFirstLine;
	private final String lineDelimiter;

	private long readCount;
	private long processedCount;

	private final S3SelectClient selectClient;
	private final SelectObjectContentRequest selectObjectContentRequest;

	private LineReader lineReader;
	private InputStream selectRecordsInputStream;

	public S3SelectCsvReader(
			Configuration configuration,
			FileSplit fileSplit,
			Properties properties,
			String ionSqlQuery) {
		this.start = fileSplit.getStart();
		this.end = this.start + fileSplit.getLength();
		this.pos = this.start;

		this.isFirstLine = true;
		this.lineDelimiter = properties.getProperty(LINE_DELIM, "\n");

		this.selectClient = new S3SelectClient(configuration);
		this.selectObjectContentRequest = getSelectObjectContentRequest(
				properties,
				ionSqlQuery,
				fileSplit.getPath(),
				configuration);
	}

	public SelectObjectContentRequest getSelectObjectContentRequest(
			Properties properties,
			String query,
			Path path,
			Configuration configuration) {
		if (path == null || !path.isAbsolute()) {
			throw new IllegalArgumentException("Path = " + path + " is not absolute path");
		}

		//Get bucket from uri
		String bucket;
		URI uri = path.toUri();
		if (uri.getHost() != null) {
			bucket = uri.getHost();
		} else if (uri.getUserInfo() == null) {
			bucket = uri.getAuthority();
		} else {
			throw new IllegalArgumentException("uri = " + uri + " is invalid");
		}

		//Get key from uri
		String key = uri.getPath();
		if (key == null) {
			key = "";
		} else {
			if (key.startsWith(SEPARATOR)) {
				key = key.substring(SEPARATOR.length());
			}
			if (key.endsWith(SEPARATOR)) {
				key = key.substring(0, key.length() - SEPARATOR.length());
			}
		}

		//Create InputSerialization and OutputSerialization
		String fieldDelimiter = properties.getProperty(
				FIELD_DELIM,
				properties.getProperty(SERIALIZATION_FORMAT));
		String quoteChar = properties.getProperty(QUOTE_CHAR, null);
		String escapeChar = properties.getProperty(ESCAPE_CHAR, null);
		CSVInput csvInput = new CSVInput().withRecordDelimiter(lineDelimiter)
				.withFieldDelimiter(fieldDelimiter)
				.withComments("#")
				.withQuoteCharacter(quoteChar)
				.withQuoteEscapeCharacter(escapeChar);
		CSVOutput csvOutput = new CSVOutput()
				.withRecordDelimiter(lineDelimiter)
				.withFieldDelimiter(fieldDelimiter)
				.withQuoteCharacter(quoteChar).withQuoteEscapeCharacter(escapeChar);

		CompressionType compressionType;
		CompressionCodec codec = new CompressionCodecFactory(configuration).getCodec(path);
		if (codec == null) {
			compressionType = CompressionType.NONE;
		} else if (codec instanceof GzipCodec) {
			compressionType = CompressionType.GZIP;
		} else if (codec instanceof BZip2Codec) {
			compressionType = CompressionType.BZIP2;
		} else {
			throw new RuntimeException("Not supported compression for S3 Select: " + path);
		}

		InputSerialization inputSerialization = new InputSerialization()
				.withCompressionType(compressionType)
				.withCsv(csvInput);
		OutputSerialization outputSerialization = new OutputSerialization().withCsv(csvOutput);

		return new SelectObjectContentRequest()
				.withBucketName(bucket)
				.withKey(key)
				.withExpression(query)
				.withExpressionType(ExpressionType.SQL)
				.withInputSerialization(inputSerialization)
				.withOutputSerialization(outputSerialization);
	}

	private int readLine(Text value) {
		try {
			if (isFirstLine) {
				isFirstLine = false;
				readCount = 0;

				selectRecordsInputStream = selectClient.getSelectRecordsInputStream(selectObjectContentRequest);
				selectRecordsInputStream.skip(start);
				lineReader = new LineReader(selectRecordsInputStream, lineDelimiter.getBytes(StandardCharsets.UTF_8));
			}

			return lineReader.readLine(value);
		} catch (Exception e) {
			isFirstLine = true;
			readCount = 0;
			throw new RuntimeException(e);
		}
	}

	@Override
	public synchronized boolean next(Writable key, Writable value) throws IOException {
		while (true) {
			int bytes = readLine((Text) value);
			if (bytes <= 0) {
				if (!selectClient.isReceivedEndEvent()) {
					throw new IOException("EndEvent was not received, not finish!");
				}
				return false;
			}

			readCount++;
			if (readCount > processedCount) {
				//update pos and processedCount
				pos += bytes;

				processedCount++;
				((LongWritable) key).set(processedCount);
				return true;
			}
		}
	}

	@Override
	public LongWritable createKey() {
		return new LongWritable();
	}

	@Override
	public Text createValue() {
		return new Text();
	}

	@Override
	public long getPos() {
		return pos;
	}

	@Override
	public void close() throws IOException {
		if (selectRecordsInputStream != null) {
			selectRecordsInputStream.close();
		}

		if (lineReader != null) {
			lineReader.close();
		}

		if (selectClient != null) {
			selectClient.close();
		}
	}

	@Override
	public float getProgress() {
		return ((float) (pos - start)) / (end - start);
	}
}
