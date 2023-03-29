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

<<<<<<< HEAD   (c107c4 DX-64051: Bump jettison from 1.1 to 1.5.4 in hadoop/2_8_5_az)
=======
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.ACQUIRE_LEASE_ACTION;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.APN_VERSION;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.APPEND_ACTION;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.APPEND_BLOB_TYPE;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.APPLICATION_JSON;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.APPLICATION_OCTET_STREAM;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.BREAK_LEASE_ACTION;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.CHECK_ACCESS;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.CLIENT_VERSION;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.COMMA;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.DEFAULT_LEASE_BREAK_PERIOD;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.DEFAULT_TIMEOUT;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.DIRECTORY;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.EMPTY_STRING;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.FILE;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.FILESYSTEM;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.FLUSH_ACTION;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.FORWARD_SLASH;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.FORWARD_SLASH_ENCODE;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.HTTP_METHOD_DELETE;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.HTTP_METHOD_GET;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.HTTP_METHOD_HEAD;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.HTTP_METHOD_PATCH;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.HTTP_METHOD_POST;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.HTTP_METHOD_PUT;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.JAVA_VENDOR;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.JAVA_VERSION;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.OS_ARCH;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.OS_NAME;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.OS_VERSION;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.PLUS;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.PLUS_ENCODE;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.RELEASE_LEASE_ACTION;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.RENEW_LEASE_ACTION;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.SEMICOLON;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.SET_PROPERTIES_ACTION;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.SINGLE_WHITE_SPACE;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.STAR;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.TRUE;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.UTF_8;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.DEFAULT_DELETE_CONSIDERED_IDEMPOTENT;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.SERVER_SIDE_ENCRYPTION_ALGORITHM;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemUriSchemes.HTTPS_SCHEME;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.ACCEPT;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.ACCEPT_CHARSET;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.CONTENT_TYPE;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.IF_MATCH;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.IF_NONE_MATCH;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.RANGE;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.USER_AGENT;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.X_HTTP_METHOD_OVERRIDE;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.X_MS_ENCRYPTION_ALGORITHM;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.X_MS_ENCRYPTION_KEY;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.X_MS_ENCRYPTION_KEY_SHA256;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.X_MS_EXISTING_RESOURCE_TYPE;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.X_MS_LEASE_ACTION;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.X_MS_LEASE_BREAK_PERIOD;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.X_MS_LEASE_DURATION;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.X_MS_LEASE_ID;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.X_MS_PROPERTIES;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.X_MS_PROPOSED_LEASE_ID;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.X_MS_RENAME_SOURCE;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.X_MS_VERSION;
import static org.apache.hadoop.fs.azurebfs.constants.HttpQueryParams.QUERY_FS_ACTION;
import static org.apache.hadoop.fs.azurebfs.constants.HttpQueryParams.QUERY_PARAM_ACTION;
import static org.apache.hadoop.fs.azurebfs.constants.HttpQueryParams.QUERY_PARAM_BLOBTYPE;
import static org.apache.hadoop.fs.azurebfs.constants.HttpQueryParams.QUERY_PARAM_CLOSE;
import static org.apache.hadoop.fs.azurebfs.constants.HttpQueryParams.QUERY_PARAM_CONTINUATION;
import static org.apache.hadoop.fs.azurebfs.constants.HttpQueryParams.QUERY_PARAM_DIRECTORY;
import static org.apache.hadoop.fs.azurebfs.constants.HttpQueryParams.QUERY_PARAM_FLUSH;
import static org.apache.hadoop.fs.azurebfs.constants.HttpQueryParams.QUERY_PARAM_MAXRESULTS;
import static org.apache.hadoop.fs.azurebfs.constants.HttpQueryParams.QUERY_PARAM_POSITION;
import static org.apache.hadoop.fs.azurebfs.constants.HttpQueryParams.QUERY_PARAM_RECURSIVE;
import static org.apache.hadoop.fs.azurebfs.constants.HttpQueryParams.QUERY_PARAM_RESOURCE;
import static org.apache.hadoop.fs.azurebfs.constants.HttpQueryParams.QUERY_PARAM_RETAIN_UNCOMMITTED_DATA;
import static org.apache.hadoop.fs.azurebfs.constants.HttpQueryParams.QUERY_PARAM_TIMEOUT;

import java.io.Closeable;
>>>>>>> CHANGE (74cfc9 WIP: Allow for custom shared key signer for ABFS)
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

<<<<<<< HEAD   (c107c4 DX-64051: Bump jettison from 1.1 to 1.5.4 in hadoop/2_8_5_az)
import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.fs.azurebfs.utils.SSLSocketFactoryEx;
import org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants;
import org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations;
import org.apache.hadoop.fs.azurebfs.constants.HttpQueryParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.InvalidUriException;
import org.apache.hadoop.fs.azurebfs.AbfsConfiguration;
import org.apache.hadoop.fs.azurebfs.oauth2.AccessTokenProvider;

import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.*;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemUriSchemes.HTTPS_SCHEME;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.*;
import static org.apache.hadoop.fs.azurebfs.constants.HttpQueryParams.*;
=======
import org.apache.hadoop.fs.azurebfs.AbfsConfiguration;
import org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants;
import org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations;
import org.apache.hadoop.fs.azurebfs.constants.HttpQueryParams;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.InvalidUriException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.SASTokenProviderException;
import org.apache.hadoop.fs.azurebfs.contracts.services.AppendRequestParameters;
import org.apache.hadoop.fs.azurebfs.extensions.ExtensionHelper;
import org.apache.hadoop.fs.azurebfs.extensions.SASTokenProvider;
import org.apache.hadoop.fs.azurebfs.oauth2.AccessTokenProvider;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.security.ssl.DelegatingSSLSocketFactory;
import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;
import org.apache.hadoop.thirdparty.com.google.common.base.Strings;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.FutureCallback;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.Futures;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ListenableFuture;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ListenableScheduledFuture;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ListeningScheduledExecutorService;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.MoreExecutors;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.util.concurrent.HadoopExecutors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
>>>>>>> CHANGE (74cfc9 WIP: Allow for custom shared key signer for ABFS)

/**
 * AbfsClient.
 */
public class AbfsClient {
  public static final Logger LOG = LoggerFactory.getLogger(AbfsClient.class);
  private final URL baseUrl;
<<<<<<< HEAD   (c107c4 DX-64051: Bump jettison from 1.1 to 1.5.4 in hadoop/2_8_5_az)
  private final SharedKeyCredentials sharedKeyCredentials;
  private final String xMsVersion = "2018-11-09";
=======
  private final SharedKeySigner sharedKeySigner;
  private final String xMsVersion = "2019-12-12";
>>>>>>> CHANGE (74cfc9 WIP: Allow for custom shared key signer for ABFS)
  private final ExponentialRetryPolicy retryPolicy;
  private final String filesystem;
  private final AbfsConfiguration abfsConfiguration;
  private final String userAgent;

<<<<<<< HEAD   (c107c4 DX-64051: Bump jettison from 1.1 to 1.5.4 in hadoop/2_8_5_az)
  private final AccessTokenProvider tokenProvider;
=======
  private final String accountName;
  private final AuthType authType;
  private final AccessTokenProvider tokenProvider;
  private final SASTokenProvider sasTokenProvider;
  private final AbfsCounters abfsCounters;
>>>>>>> CHANGE (74cfc9 WIP: Allow for custom shared key signer for ABFS)


<<<<<<< HEAD   (c107c4 DX-64051: Bump jettison from 1.1 to 1.5.4 in hadoop/2_8_5_az)
  public AbfsClient(final URL baseUrl, final SharedKeyCredentials sharedKeyCredentials,
=======
  private AbfsClient(final URL baseUrl, final SharedKeySigner sharedKeySigner,
>>>>>>> CHANGE (74cfc9 WIP: Allow for custom shared key signer for ABFS)
                    final AbfsConfiguration abfsConfiguration,
<<<<<<< HEAD   (c107c4 DX-64051: Bump jettison from 1.1 to 1.5.4 in hadoop/2_8_5_az)
                    final ExponentialRetryPolicy exponentialRetryPolicy,
                    final AccessTokenProvider tokenProvider) {
=======
                    final AbfsClientContext abfsClientContext,
                    final AccessTokenProvider tokenProvider,
                    final SASTokenProvider sasTokenProvider
                    )
      throws IOException {
>>>>>>> CHANGE (74cfc9 WIP: Allow for custom shared key signer for ABFS)
    this.baseUrl = baseUrl;
    this.sharedKeySigner = sharedKeySigner;
    this.tokenProvider = tokenProvider;
    this.sasTokenProvider = sasTokenProvider;
    String baseUrlString = baseUrl.toString();
    this.filesystem = baseUrlString.substring(baseUrlString.lastIndexOf(FORWARD_SLASH) + 1);
    this.abfsConfiguration = abfsConfiguration;
    this.retryPolicy = exponentialRetryPolicy;

    String sslProviderName = null;

    if (this.baseUrl.toString().startsWith(HTTPS_SCHEME)) {
      try {
        SSLSocketFactoryEx.initializeDefaultFactory(this.abfsConfiguration.getPreferredSSLFactoryOption());
        sslProviderName = SSLSocketFactoryEx.getDefaultFactory().getProviderName();
      } catch (IOException e) {
        // Suppress exception. Failure to init SSLSocketFactoryEx would have only performance impact.
      }
    }

    this.userAgent = initializeUserAgent(abfsConfiguration, sslProviderName);
<<<<<<< HEAD   (c107c4 DX-64051: Bump jettison from 1.1 to 1.5.4 in hadoop/2_8_5_az)
    this.tokenProvider = tokenProvider;
=======
    this.abfsPerfTracker = abfsClientContext.getAbfsPerfTracker();
    this.abfsCounters = abfsClientContext.getAbfsCounters();

    ThreadFactory tf =
        new ThreadFactoryBuilder().setNameFormat("AbfsClient Lease Ops").setDaemon(true).build();
    this.executorService = MoreExecutors.listeningDecorator(
        HadoopExecutors.newScheduledThreadPool(this.abfsConfiguration.getNumLeaseThreads(), tf));
  }

  public AbfsClient(final URL baseUrl, final SharedKeyCredentials sharedKeyCredentials,
                    final AbfsConfiguration abfsConfiguration,
                    final AccessTokenProvider tokenProvider,
                    final AbfsClientContext abfsClientContext)
      throws IOException {
    this(baseUrl, null, abfsConfiguration, abfsClientContext, tokenProvider, null);
>>>>>>> CHANGE (74cfc9 WIP: Allow for custom shared key signer for ABFS)
  }

<<<<<<< HEAD   (c107c4 DX-64051: Bump jettison from 1.1 to 1.5.4 in hadoop/2_8_5_az)
=======
  public AbfsClient(final URL baseUrl, final SharedKeyCredentials sharedKeyCredentials,
                    final AbfsConfiguration abfsConfiguration,
                    final SASTokenProvider sasTokenProvider,
                    final AbfsClientContext abfsClientContext)
      throws IOException {
    this(baseUrl, null, abfsConfiguration, abfsClientContext, null, sasTokenProvider);
  }

  public AbfsClient(final URL baseUrl, final SharedKeySigner sharedKeySigner,
      final AbfsConfiguration abfsConfiguration,
      final AbfsClientContext abfsClientContext)
          throws IOException {
    this(baseUrl, sharedKeySigner, abfsConfiguration, abfsClientContext, null, null);
  }

  private byte[] getSHA256Hash(String key) throws IOException {
    try {
      final MessageDigest digester = MessageDigest.getInstance("SHA-256");
      return digester.digest(key.getBytes(StandardCharsets.UTF_8));
    } catch (NoSuchAlgorithmException e) {
      throw new IOException(e);
    }
  }

  private String getBase64EncodedString(String key) {
    return getBase64EncodedString(key.getBytes(StandardCharsets.UTF_8));
  }

  private String getBase64EncodedString(byte[] bytes) {
    return Base64.getEncoder().encodeToString(bytes);
  }

  @Override
  public void close() throws IOException {
    if (tokenProvider instanceof Closeable) {
      IOUtils.cleanupWithLogger(LOG, (Closeable) tokenProvider);
    }
    HadoopExecutors.shutdown(executorService, LOG, 0, TimeUnit.SECONDS);
  }

>>>>>>> CHANGE (74cfc9 WIP: Allow for custom shared key signer for ABFS)
  public String getFileSystem() {
    return filesystem;
  }

  ExponentialRetryPolicy getRetryPolicy() {
    return retryPolicy;
  }


  SharedKeySigner getSharedKeySigner() {
    return sharedKeySigner;
  }

  List<AbfsHttpHeader> createDefaultHeaders() {
    final List<AbfsHttpHeader> requestHeaders = new ArrayList<AbfsHttpHeader>();
    requestHeaders.add(new AbfsHttpHeader(X_MS_VERSION, xMsVersion));
    requestHeaders.add(new AbfsHttpHeader(ACCEPT, APPLICATION_JSON
            + COMMA + SINGLE_WHITE_SPACE + APPLICATION_OCTET_STREAM));
    requestHeaders.add(new AbfsHttpHeader(ACCEPT_CHARSET,
            UTF_8));
    requestHeaders.add(new AbfsHttpHeader(CONTENT_TYPE, EMPTY_STRING));
    requestHeaders.add(new AbfsHttpHeader(USER_AGENT, userAgent));
    return requestHeaders;
  }

  AbfsUriQueryBuilder createDefaultUriQueryBuilder() {
    final AbfsUriQueryBuilder abfsUriQueryBuilder = new AbfsUriQueryBuilder();
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_TIMEOUT, DEFAULT_TIMEOUT);
    return abfsUriQueryBuilder;
  }

  public AbfsRestOperation createFilesystem() throws AzureBlobFileSystemException {
    final List<AbfsHttpHeader> requestHeaders = createDefaultHeaders();

    final AbfsUriQueryBuilder abfsUriQueryBuilder = new AbfsUriQueryBuilder();
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_RESOURCE, FILESYSTEM);

    final URL url = createRequestUrl(abfsUriQueryBuilder.toString());
    final AbfsRestOperation op = new AbfsRestOperation(
            AbfsRestOperationType.CreateFileSystem,
            this,
            HTTP_METHOD_PUT,
            url,
            requestHeaders);
    op.execute();
    return op;
  }

  public AbfsRestOperation setFilesystemProperties(final String properties) throws AzureBlobFileSystemException {
    final List<AbfsHttpHeader> requestHeaders = createDefaultHeaders();
    // JDK7 does not support PATCH, so to workaround the issue we will use
    // PUT and specify the real method in the X-Http-Method-Override header.
    requestHeaders.add(new AbfsHttpHeader(X_HTTP_METHOD_OVERRIDE,
            HTTP_METHOD_PATCH));

    requestHeaders.add(new AbfsHttpHeader(X_MS_PROPERTIES,
            properties));

    final AbfsUriQueryBuilder abfsUriQueryBuilder = createDefaultUriQueryBuilder();
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_RESOURCE, FILESYSTEM);

    final URL url = createRequestUrl(abfsUriQueryBuilder.toString());
    final AbfsRestOperation op = new AbfsRestOperation(
            AbfsRestOperationType.SetFileSystemProperties,
            this,
            HTTP_METHOD_PUT,
            url,
            requestHeaders);
    op.execute();
    return op;
  }

  public AbfsRestOperation listPath(final String relativePath, final boolean recursive, final int listMaxResults,
                                    final String continuation) throws AzureBlobFileSystemException {
    final List<AbfsHttpHeader> requestHeaders = createDefaultHeaders();

    final AbfsUriQueryBuilder abfsUriQueryBuilder = createDefaultUriQueryBuilder();
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_RESOURCE, FILESYSTEM);
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_DIRECTORY, relativePath == null ? AbfsHttpConstants.EMPTY_STRING
        : relativePath);
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_RECURSIVE, String.valueOf(recursive));
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_CONTINUATION, continuation);
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_MAXRESULTS, String.valueOf(listMaxResults));
    abfsUriQueryBuilder.addQuery(HttpQueryParams.QUERY_PARAM_UPN, String.valueOf(abfsConfiguration.isUpnUsed()));

    final URL url = createRequestUrl(abfsUriQueryBuilder.toString());
    final AbfsRestOperation op = new AbfsRestOperation(
            AbfsRestOperationType.ListPaths,
            this,
            HTTP_METHOD_GET,
            url,
            requestHeaders);
    op.execute();
    return op;
  }

  public AbfsRestOperation getFilesystemProperties() throws AzureBlobFileSystemException {
    final List<AbfsHttpHeader> requestHeaders = createDefaultHeaders();

    final AbfsUriQueryBuilder abfsUriQueryBuilder = createDefaultUriQueryBuilder();
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_RESOURCE, FILESYSTEM);

    final URL url = createRequestUrl(abfsUriQueryBuilder.toString());
    final AbfsRestOperation op = new AbfsRestOperation(
            AbfsRestOperationType.GetFileSystemProperties,
            this,
            HTTP_METHOD_HEAD,
            url,
            requestHeaders);
    op.execute();
    return op;
  }

  public AbfsRestOperation deleteFilesystem() throws AzureBlobFileSystemException {
    final List<AbfsHttpHeader> requestHeaders = createDefaultHeaders();

    final AbfsUriQueryBuilder abfsUriQueryBuilder = createDefaultUriQueryBuilder();
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_RESOURCE, FILESYSTEM);

    final URL url = createRequestUrl(abfsUriQueryBuilder.toString());
    final AbfsRestOperation op = new AbfsRestOperation(
            AbfsRestOperationType.DeleteFileSystem,
            this,
            HTTP_METHOD_DELETE,
            url,
            requestHeaders);
    op.execute();
    return op;
  }

  public AbfsRestOperation createPath(final String path, final boolean isFile, final boolean overwrite,
                                      final String permission, final String umask) throws AzureBlobFileSystemException {
    final List<AbfsHttpHeader> requestHeaders = createDefaultHeaders();
    if (!overwrite) {
      requestHeaders.add(new AbfsHttpHeader(IF_NONE_MATCH, AbfsHttpConstants.STAR));
    }

    if (permission != null && !permission.isEmpty()) {
      requestHeaders.add(new AbfsHttpHeader(HttpHeaderConfigurations.X_MS_PERMISSIONS, permission));
    }

    if (umask != null && !umask.isEmpty()) {
      requestHeaders.add(new AbfsHttpHeader(HttpHeaderConfigurations.X_MS_UMASK, umask));
    }

    final AbfsUriQueryBuilder abfsUriQueryBuilder = createDefaultUriQueryBuilder();
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_RESOURCE, isFile ? FILE : DIRECTORY);

    final URL url = createRequestUrl(path, abfsUriQueryBuilder.toString());
    final AbfsRestOperation op = new AbfsRestOperation(
            AbfsRestOperationType.CreatePath,
            this,
            HTTP_METHOD_PUT,
            url,
            requestHeaders);
    op.execute();
    return op;
  }

  public AbfsRestOperation renamePath(final String source, final String destination, final String continuation)
          throws AzureBlobFileSystemException {
    final List<AbfsHttpHeader> requestHeaders = createDefaultHeaders();

    final String encodedRenameSource = urlEncode(FORWARD_SLASH + this.getFileSystem() + source);
    requestHeaders.add(new AbfsHttpHeader(X_MS_RENAME_SOURCE, encodedRenameSource));
    requestHeaders.add(new AbfsHttpHeader(IF_NONE_MATCH, STAR));

    final AbfsUriQueryBuilder abfsUriQueryBuilder = createDefaultUriQueryBuilder();
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_CONTINUATION, continuation);

    final URL url = createRequestUrl(destination, abfsUriQueryBuilder.toString());
    final AbfsRestOperation op = new AbfsRestOperation(
            AbfsRestOperationType.RenamePath,
            this,
            HTTP_METHOD_PUT,
            url,
            requestHeaders);
    op.execute();
    return op;
  }

  public AbfsRestOperation append(final String path, final long position, final byte[] buffer, final int offset,
                                  final int length) throws AzureBlobFileSystemException {
    final List<AbfsHttpHeader> requestHeaders = createDefaultHeaders();
    // JDK7 does not support PATCH, so to workaround the issue we will use
    // PUT and specify the real method in the X-Http-Method-Override header.
    requestHeaders.add(new AbfsHttpHeader(X_HTTP_METHOD_OVERRIDE,
            HTTP_METHOD_PATCH));

    final AbfsUriQueryBuilder abfsUriQueryBuilder = createDefaultUriQueryBuilder();
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_ACTION, APPEND_ACTION);
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_POSITION, Long.toString(position));

    final URL url = createRequestUrl(path, abfsUriQueryBuilder.toString());
    final AbfsRestOperation op = new AbfsRestOperation(
        AbfsRestOperationType.Append,
            this,
            HTTP_METHOD_PUT,
            url,
            requestHeaders, buffer, offset, length);
    op.execute();
    return op;
  }

  public AbfsRestOperation flush(final String path, final long position, boolean retainUncommittedData)
      throws AzureBlobFileSystemException {
    final List<AbfsHttpHeader> requestHeaders = createDefaultHeaders();
    // JDK7 does not support PATCH, so to workaround the issue we will use
    // PUT and specify the real method in the X-Http-Method-Override header.
    requestHeaders.add(new AbfsHttpHeader(X_HTTP_METHOD_OVERRIDE,
            HTTP_METHOD_PATCH));

    final AbfsUriQueryBuilder abfsUriQueryBuilder = createDefaultUriQueryBuilder();
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_ACTION, FLUSH_ACTION);
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_POSITION, Long.toString(position));
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_RETAIN_UNCOMMITTED_DATA, String.valueOf(retainUncommittedData));

    final URL url = createRequestUrl(path, abfsUriQueryBuilder.toString());
    final AbfsRestOperation op = new AbfsRestOperation(
            AbfsRestOperationType.Flush,
            this,
            HTTP_METHOD_PUT,
            url,
            requestHeaders);
    op.execute();
    return op;
  }

  public AbfsRestOperation setPathProperties(final String path, final String properties)
      throws AzureBlobFileSystemException {
    final List<AbfsHttpHeader> requestHeaders = createDefaultHeaders();
    // JDK7 does not support PATCH, so to workaround the issue we will use
    // PUT and specify the real method in the X-Http-Method-Override header.
    requestHeaders.add(new AbfsHttpHeader(X_HTTP_METHOD_OVERRIDE,
            HTTP_METHOD_PATCH));

    requestHeaders.add(new AbfsHttpHeader(X_MS_PROPERTIES, properties));

    final AbfsUriQueryBuilder abfsUriQueryBuilder = createDefaultUriQueryBuilder();
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_ACTION, SET_PROPERTIES_ACTION);

    final URL url = createRequestUrl(path, abfsUriQueryBuilder.toString());
    final AbfsRestOperation op = new AbfsRestOperation(
            AbfsRestOperationType.SetPathProperties,
            this,
            HTTP_METHOD_PUT,
            url,
            requestHeaders);
    op.execute();
    return op;
  }

  public AbfsRestOperation getPathProperties(final String path, boolean isNameSpaceEnabled) throws AzureBlobFileSystemException {
    final List<AbfsHttpHeader> requestHeaders = createDefaultHeaders();

    final AbfsUriQueryBuilder abfsUriQueryBuilder = createDefaultUriQueryBuilder();
    abfsUriQueryBuilder.addQuery(HttpQueryParams.QUERY_PARAM_UPN, String.valueOf(abfsConfiguration.isUpnUsed()));

    final URL url = createRequestUrl(path, abfsUriQueryBuilder.toString());
    final AbfsRestOperation op = new AbfsRestOperation(
            isNameSpaceEnabled ? AbfsRestOperationType.GetPathProperties : AbfsRestOperationType.GetPathStatus,
            this,
            HTTP_METHOD_HEAD,
            url,
            requestHeaders);
    op.execute();
    return op;
  }

  public AbfsRestOperation read(final String path, final long position, final byte[] buffer, final int bufferOffset,
                                final int bufferLength, final String eTag) throws AzureBlobFileSystemException {
    final List<AbfsHttpHeader> requestHeaders = createDefaultHeaders();
    requestHeaders.add(new AbfsHttpHeader(RANGE,
            String.format("bytes=%d-%d", position, position + bufferLength - 1)));
    requestHeaders.add(new AbfsHttpHeader(IF_MATCH, eTag));

    final AbfsUriQueryBuilder abfsUriQueryBuilder = createDefaultUriQueryBuilder();

    final URL url = createRequestUrl(path, abfsUriQueryBuilder.toString());

    final AbfsRestOperation op = new AbfsRestOperation(
            AbfsRestOperationType.ReadFile,
            this,
            HTTP_METHOD_GET,
            url,
            requestHeaders,
            buffer,
            bufferOffset,
            bufferLength);
    op.execute();

    return op;
  }

  public AbfsRestOperation deletePath(final String path, final boolean recursive, final String continuation)
          throws AzureBlobFileSystemException {
    final List<AbfsHttpHeader> requestHeaders = createDefaultHeaders();

    final AbfsUriQueryBuilder abfsUriQueryBuilder = createDefaultUriQueryBuilder();
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_RECURSIVE, String.valueOf(recursive));
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_CONTINUATION, continuation);

    final URL url = createRequestUrl(path, abfsUriQueryBuilder.toString());
    final AbfsRestOperation op = new AbfsRestOperation(
            AbfsRestOperationType.DeletePath,
            this,
            HTTP_METHOD_DELETE,
            url,
            requestHeaders);
    op.execute();
    return op;
  }

  public AbfsRestOperation setOwner(final String path, final String owner, final String group)
      throws AzureBlobFileSystemException {
    final List<AbfsHttpHeader> requestHeaders = createDefaultHeaders();
    // JDK7 does not support PATCH, so to workaround the issue we will use
    // PUT and specify the real method in the X-Http-Method-Override header.
    requestHeaders.add(new AbfsHttpHeader(X_HTTP_METHOD_OVERRIDE,
            HTTP_METHOD_PATCH));

    if (owner != null && !owner.isEmpty()) {
      requestHeaders.add(new AbfsHttpHeader(HttpHeaderConfigurations.X_MS_OWNER, owner));
    }
    if (group != null && !group.isEmpty()) {
      requestHeaders.add(new AbfsHttpHeader(HttpHeaderConfigurations.X_MS_GROUP, group));
    }

    final AbfsUriQueryBuilder abfsUriQueryBuilder = createDefaultUriQueryBuilder();
    abfsUriQueryBuilder.addQuery(HttpQueryParams.QUERY_PARAM_ACTION, AbfsHttpConstants.SET_ACCESS_CONTROL);

    final URL url = createRequestUrl(path, abfsUriQueryBuilder.toString());
    final AbfsRestOperation op = new AbfsRestOperation(
        AbfsRestOperationType.SetOwner,
        this,
        AbfsHttpConstants.HTTP_METHOD_PUT,
        url,
        requestHeaders);
    op.execute();
    return op;
  }

  public AbfsRestOperation setPermission(final String path, final String permission)
      throws AzureBlobFileSystemException {
    final List<AbfsHttpHeader> requestHeaders = createDefaultHeaders();
    // JDK7 does not support PATCH, so to workaround the issue we will use
    // PUT and specify the real method in the X-Http-Method-Override header.
    requestHeaders.add(new AbfsHttpHeader(X_HTTP_METHOD_OVERRIDE,
            HTTP_METHOD_PATCH));

    requestHeaders.add(new AbfsHttpHeader(HttpHeaderConfigurations.X_MS_PERMISSIONS, permission));

    final AbfsUriQueryBuilder abfsUriQueryBuilder = createDefaultUriQueryBuilder();
    abfsUriQueryBuilder.addQuery(HttpQueryParams.QUERY_PARAM_ACTION, AbfsHttpConstants.SET_ACCESS_CONTROL);

    final URL url = createRequestUrl(path, abfsUriQueryBuilder.toString());
    final AbfsRestOperation op = new AbfsRestOperation(
        AbfsRestOperationType.SetPermissions,
        this,
        AbfsHttpConstants.HTTP_METHOD_PUT,
        url,
        requestHeaders);
    op.execute();
    return op;
  }

  public AbfsRestOperation setAcl(final String path, final String aclSpecString) throws AzureBlobFileSystemException {
    return setAcl(path, aclSpecString, AbfsHttpConstants.EMPTY_STRING);
  }

  public AbfsRestOperation setAcl(final String path, final String aclSpecString, final String eTag)
      throws AzureBlobFileSystemException {
    final List<AbfsHttpHeader> requestHeaders = createDefaultHeaders();
    // JDK7 does not support PATCH, so to workaround the issue we will use
    // PUT and specify the real method in the X-Http-Method-Override header.
    requestHeaders.add(new AbfsHttpHeader(X_HTTP_METHOD_OVERRIDE,
            HTTP_METHOD_PATCH));

    requestHeaders.add(new AbfsHttpHeader(HttpHeaderConfigurations.X_MS_ACL, aclSpecString));

    if (eTag != null && !eTag.isEmpty()) {
      requestHeaders.add(new AbfsHttpHeader(HttpHeaderConfigurations.IF_MATCH, eTag));
    }

    final AbfsUriQueryBuilder abfsUriQueryBuilder = createDefaultUriQueryBuilder();
    abfsUriQueryBuilder.addQuery(HttpQueryParams.QUERY_PARAM_ACTION, AbfsHttpConstants.SET_ACCESS_CONTROL);

    final URL url = createRequestUrl(path, abfsUriQueryBuilder.toString());
    final AbfsRestOperation op = new AbfsRestOperation(
        AbfsRestOperationType.SetAcl,
        this,
        AbfsHttpConstants.HTTP_METHOD_PUT,
        url,
        requestHeaders);
    op.execute();
    return op;
  }

  public AbfsRestOperation getAclStatus(final String path) throws AzureBlobFileSystemException {
    return getAclStatus(path, abfsConfiguration.isUpnUsed());
  }

  public AbfsRestOperation getAclStatus(final String path, final boolean useUPN) throws AzureBlobFileSystemException {
    final List<AbfsHttpHeader> requestHeaders = createDefaultHeaders();

    final AbfsUriQueryBuilder abfsUriQueryBuilder = createDefaultUriQueryBuilder();
    abfsUriQueryBuilder.addQuery(HttpQueryParams.QUERY_PARAM_ACTION, AbfsHttpConstants.GET_ACCESS_CONTROL);
    abfsUriQueryBuilder.addQuery(HttpQueryParams.QUERY_PARAM_UPN, String.valueOf(useUPN));

    final URL url = createRequestUrl(path, abfsUriQueryBuilder.toString());
    final AbfsRestOperation op = new AbfsRestOperation(
        AbfsRestOperationType.GetAcl,
        this,
        AbfsHttpConstants.HTTP_METHOD_HEAD,
        url,
        requestHeaders);
    op.execute();
    return op;
  }

  private URL createRequestUrl(final String query) throws AzureBlobFileSystemException {
    return createRequestUrl(EMPTY_STRING, query);
  }

  private URL createRequestUrl(final String path, final String query)
          throws AzureBlobFileSystemException {
    final String base = baseUrl.toString();
    String encodedPath = path;
    try {
      encodedPath = urlEncode(path);
    } catch (AzureBlobFileSystemException ex) {
      LOG.debug("Unexpected error.", ex);
      throw new InvalidUriException(path);
    }

    final StringBuilder sb = new StringBuilder();
    sb.append(base);
    sb.append(encodedPath);
    sb.append(query);

    final URL url;
    try {
      url = new URL(sb.toString());
    } catch (MalformedURLException ex) {
      throw new InvalidUriException(sb.toString());
    }
    return url;
  }

  public static String urlEncode(final String value) throws AzureBlobFileSystemException {
    String encodedString;
    try {
      encodedString =  URLEncoder.encode(value, UTF_8)
          .replace(PLUS, PLUS_ENCODE)
          .replace(FORWARD_SLASH_ENCODE, FORWARD_SLASH);
    } catch (UnsupportedEncodingException ex) {
        throw new InvalidUriException(value);
    }

    return encodedString;
  }

  public synchronized String getAccessToken() throws IOException {
    if (tokenProvider != null) {
      return "Bearer " + tokenProvider.getToken().getAccessToken();
    } else {
      return null;
    }
  }

  @VisibleForTesting
  String initializeUserAgent(final AbfsConfiguration abfsConfiguration,
                             final String sslProviderName) {
    StringBuilder sb = new StringBuilder();
    sb.append("(JavaJRE ");
    sb.append(System.getProperty(JAVA_VERSION));
    sb.append("; ");
    sb.append(
        System.getProperty(OS_NAME).replaceAll(SINGLE_WHITE_SPACE, EMPTY_STRING));
    sb.append(" ");
    sb.append(System.getProperty(OS_VERSION));
    if (sslProviderName != null && !sslProviderName.isEmpty()) {
      sb.append("; ");
      sb.append(sslProviderName);
    }
    sb.append(")");
    final String userAgentComment = sb.toString();
    String customUserAgentId = abfsConfiguration.getCustomUserAgentPrefix();
    if (customUserAgentId != null && !customUserAgentId.isEmpty()) {
      return String.format(Locale.ROOT, CLIENT_VERSION + " %s %s",
          userAgentComment, customUserAgentId);
    }
    return String.format(CLIENT_VERSION + " %s", userAgentComment);
  }

  @VisibleForTesting
  URL getBaseUrl() {
    return baseUrl;
  }
}
