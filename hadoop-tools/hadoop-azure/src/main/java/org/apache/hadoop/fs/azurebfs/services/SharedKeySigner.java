/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.fs.azurebfs.services;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
/**
 * Provides signing function when using Shared key to access Azure Storage
 * account.
 */
public interface SharedKeySigner {
    /**
     * Sign the provided request and attach the signature back to the request
     *
     * @param connection the connection executing the current request
     * @param contentLength the lenght of the content body
     */
    void signRequest(HttpURLConnection connection, final long contentLength) throws UnsupportedEncodingException;
}