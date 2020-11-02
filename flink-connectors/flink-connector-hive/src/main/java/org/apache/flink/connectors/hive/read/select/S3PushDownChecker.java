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

import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.mapred.TextInputFormat;

import java.util.Properties;

/**
 * Validate S3 Select push down conditions.
 */
public final class S3PushDownChecker {

	public static final String FILE_INPUT_FORMAT = "file.inputformat";
	public static final String SERIALIZATION_LIB = "serialization.lib";
	public static final String SKIP_HEADER_LINE_COUNT = "skip.header.line.count";
	public static final String SKIP_FOOTER_LINE_COUNT = "skip.footer.line.count";
	public static final String DEFAULT_SKIP_LINE_COUNT = "0";

	public static boolean shouldS3SelectPushDown(String path, Properties properties) {
		if (path != null && (path.startsWith("s3://") || path.startsWith("s3a://") || path.startsWith("s3n://"))) {
			String serDerName = properties.getProperty(SERIALIZATION_LIB);
			if (LazySimpleSerDe.class.getName().equals(serDerName)) {
				String inputFormat = properties.getProperty(FILE_INPUT_FORMAT);
				if (TextInputFormat.class.getName().equals(inputFormat) &&
					!DEFAULT_SKIP_LINE_COUNT.equals(properties.getProperty(SKIP_HEADER_LINE_COUNT)) &&
					!DEFAULT_SKIP_LINE_COUNT.equals(properties.getProperty(SKIP_FOOTER_LINE_COUNT))) {
					return true;
				}
			}
		}
		return false;
	}
}
