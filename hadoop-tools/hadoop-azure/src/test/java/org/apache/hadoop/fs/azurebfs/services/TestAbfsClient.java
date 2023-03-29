/**
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

package org.apache.hadoop.fs.azurebfs.services;

import java.net.URL;
import java.util.regex.Pattern;

import org.junit.Assert;
import org.junit.Test;

import org.apache.hadoop.fs.azurebfs.AbfsConfiguration;
import org.apache.hadoop.fs.azurebfs.utils.SSLSocketFactoryEx;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys;
import org.apache.hadoop.util.VersionInfo;

/**
 * Test useragent of abfs client.
 *
 */
public final class TestAbfsClient {

  private final String accountName = "bogusAccountName";

  private void validateUserAgent(String expectedPattern,
                                 URL baseUrl,
                                 AbfsConfiguration config,
                                 boolean includeSSLProvider) {
    AbfsClient client = new AbfsClient(baseUrl, null,
        config, null, null);
    String sslProviderName = null;
    if (includeSSLProvider) {
      sslProviderName = SSLSocketFactoryEx.getDefaultFactory().getProviderName();
    }
    String userAgent = client.initializeUserAgent(config, sslProviderName);
    Pattern pattern = Pattern.compile(expectedPattern);
    Assert.assertTrue("Incorrect User Agent String",
        pattern.matcher(userAgent).matches());
  }

  @Test
  public void verifyUnknownUserAgent() throws Exception {
    String clientVersion = "Azure Blob FS/" + VersionInfo.getVersion();
    String expectedUserAgentPattern = String.format(clientVersion
        + " %s", "\\(JavaJRE ([^\\)]+)\\)");
    final Configuration configuration = new Configuration();
    configuration.unset(ConfigurationKeys.FS_AZURE_USER_AGENT_PREFIX_KEY);
    AbfsConfiguration abfsConfiguration = new AbfsConfiguration(configuration, accountName);
    validateUserAgent(expectedUserAgentPattern, new URL("http://azure.com"),
        abfsConfiguration, false);
  }

  @Test
  public void verifyUserAgent() throws Exception {
    String clientVersion = "Azure Blob FS/" + VersionInfo.getVersion();
    String expectedUserAgentPattern = String.format(clientVersion
        + " %s", "\\(JavaJRE ([^\\)]+)\\) Partner Service");
    final Configuration configuration = new Configuration();
    configuration.set(ConfigurationKeys.FS_AZURE_USER_AGENT_PREFIX_KEY, "Partner Service");
    AbfsConfiguration abfsConfiguration = new AbfsConfiguration(configuration, accountName);
    validateUserAgent(expectedUserAgentPattern, new URL("http://azure.com"),
        abfsConfiguration, false);
  }

  @Test
  public void verifyUserAgentWithSSLProvider() throws Exception {
    String clientVersion = "Azure Blob FS/" + VersionInfo.getVersion();
    String expectedUserAgentPattern = String.format(clientVersion
        + " %s", "\\(JavaJRE ([^\\)]+)\\) Partner Service");
    final Configuration configuration = new Configuration();
    configuration.set(ConfigurationKeys.FS_AZURE_USER_AGENT_PREFIX_KEY, "Partner Service");
    configuration.set(ConfigurationKeys.FS_AZURE_SSL_CHANNEL_MODE_KEY,
<<<<<<< HEAD   (c107c4 DX-64051: Bump jettison from 1.1 to 1.5.4 in hadoop/2_8_5_az)
        SSLSocketFactoryEx.SSLChannelMode.Default_JSSE.name());
    AbfsConfiguration abfsConfiguration = new AbfsConfiguration(configuration, accountName);
    validateUserAgent(expectedUserAgentPattern, new URL("https://azure.com"),
        abfsConfiguration, true);
=======
        DelegatingSSLSocketFactory.SSLChannelMode.Default_JSSE.name());
    AbfsConfiguration abfsConfiguration = new AbfsConfiguration(configuration,
        ACCOUNT_NAME);
    String userAgentStr = getUserAgentString(abfsConfiguration, true);

    verifybBasicInfo(userAgentStr);
    assertThat(userAgentStr)
      .describedAs("User-Agent string should contain sslProvider")
      .contains(DelegatingSSLSocketFactory.getDefaultFactory().getProviderName());

    userAgentStr = getUserAgentString(abfsConfiguration, false);

    verifybBasicInfo(userAgentStr);
    assertThat(userAgentStr)
      .describedAs("User-Agent string should not contain sslProvider")
      .doesNotContain(DelegatingSSLSocketFactory.getDefaultFactory().getProviderName());
  }

  @Test
  public void verifyUserAgentClusterName() throws Exception {
    final String clusterName = "testClusterName";
    final Configuration configuration = new Configuration();
    configuration.addResource(TEST_CONFIGURATION_FILE_NAME);
    configuration.set(FS_AZURE_CLUSTER_NAME, clusterName);
    AbfsConfiguration abfsConfiguration = new AbfsConfiguration(configuration,
        ACCOUNT_NAME);
    String userAgentStr = getUserAgentString(abfsConfiguration, false);

    verifybBasicInfo(userAgentStr);
    assertThat(userAgentStr)
      .describedAs("User-Agent string should contain cluster name")
      .contains(clusterName);

    configuration.unset(FS_AZURE_CLUSTER_NAME);
    abfsConfiguration = new AbfsConfiguration(configuration,
        ACCOUNT_NAME);
    userAgentStr = getUserAgentString(abfsConfiguration, false);

    verifybBasicInfo(userAgentStr);
    assertThat(userAgentStr)
      .describedAs("User-Agent string should not contain cluster name")
      .doesNotContain(clusterName)
      .describedAs("User-Agent string should contain UNKNOWN as cluster name config is absent")
      .contains(DEFAULT_VALUE_UNKNOWN);
  }

  @Test
  public void verifyUserAgentClusterType() throws Exception {
    final String clusterType = "testClusterType";
    final Configuration configuration = new Configuration();
    configuration.addResource(TEST_CONFIGURATION_FILE_NAME);
    configuration.set(FS_AZURE_CLUSTER_TYPE, clusterType);
    AbfsConfiguration abfsConfiguration = new AbfsConfiguration(configuration,
        ACCOUNT_NAME);
    String userAgentStr = getUserAgentString(abfsConfiguration, false);

    verifybBasicInfo(userAgentStr);
    assertThat(userAgentStr)
      .describedAs("User-Agent string should contain cluster type")
      .contains(clusterType);

    configuration.unset(FS_AZURE_CLUSTER_TYPE);
    abfsConfiguration = new AbfsConfiguration(configuration,
        ACCOUNT_NAME);
    userAgentStr = getUserAgentString(abfsConfiguration, false);

    verifybBasicInfo(userAgentStr);
    assertThat(userAgentStr)
      .describedAs("User-Agent string should not contain cluster type")
      .doesNotContain(clusterType)
      .describedAs("User-Agent string should contain UNKNOWN as cluster type config is absent")
      .contains(DEFAULT_VALUE_UNKNOWN);
  }

  public static AbfsClient createTestClientFromCurrentContext(
      AbfsClient baseAbfsClientInstance,
      AbfsConfiguration abfsConfig) throws IOException {
    AuthType currentAuthType = abfsConfig.getAuthType(
        abfsConfig.getAccountName());

    AbfsPerfTracker tracker = new AbfsPerfTracker("test",
        abfsConfig.getAccountName(),
        abfsConfig);

    AbfsClientContext abfsClientContext =
        new AbfsClientContextBuilder().withAbfsPerfTracker(tracker)
                                .withExponentialRetryPolicy(
                                    new ExponentialRetryPolicy(abfsConfig.getMaxIoRetries()))
                                .build();

    // Create test AbfsClient
    AbfsClient testClient = new AbfsClient(
        baseAbfsClientInstance.getBaseUrl(),
        (currentAuthType == AuthType.SharedKey
            ? new SharedKeyCredentials(
            abfsConfig.getAccountName().substring(0,
                abfsConfig.getAccountName().indexOf(DOT)),
            abfsConfig.getStorageAccountKey())
            : null),
        abfsConfig,
        (currentAuthType == AuthType.OAuth
            ? abfsConfig.getTokenProvider()
            : null),
        abfsClientContext);

    return testClient;
  }

  public static AbfsClient getMockAbfsClient(AbfsClient baseAbfsClientInstance,
      AbfsConfiguration abfsConfig) throws Exception {
    AuthType currentAuthType = abfsConfig.getAuthType(
        abfsConfig.getAccountName());

    org.junit.Assume.assumeTrue(
        (currentAuthType == AuthType.SharedKey)
        || (currentAuthType == AuthType.OAuth));

    AbfsClient client = mock(AbfsClient.class);
    AbfsPerfTracker tracker = new AbfsPerfTracker(
        "test",
        abfsConfig.getAccountName(),
        abfsConfig);

    when(client.getAbfsPerfTracker()).thenReturn(tracker);
    when(client.getAuthType()).thenReturn(currentAuthType);
    when(client.getRetryPolicy()).thenReturn(
        new ExponentialRetryPolicy(1));

    when(client.createDefaultUriQueryBuilder()).thenCallRealMethod();
    when(client.createRequestUrl(any(), any())).thenCallRealMethod();
    when(client.getAccessToken()).thenCallRealMethod();
    when(client.getSharedKeySigner()).thenCallRealMethod();
    when(client.createDefaultHeaders()).thenCallRealMethod();

    // override baseurl
    client = TestAbfsClient.setAbfsClientField(client, "abfsConfiguration",
        abfsConfig);

    // override baseurl
    client = TestAbfsClient.setAbfsClientField(client, "baseUrl",
        baseAbfsClientInstance.getBaseUrl());

    // override auth provider
    if (currentAuthType == AuthType.SharedKey) {
      client = TestAbfsClient.setAbfsClientField(client, "sharedKeyCredentials",
          new SharedKeyCredentials(
              abfsConfig.getAccountName().substring(0,
                  abfsConfig.getAccountName().indexOf(DOT)),
              abfsConfig.getStorageAccountKey()));
    } else {
      client = TestAbfsClient.setAbfsClientField(client, "tokenProvider",
          abfsConfig.getTokenProvider());
    }

    // override user agent
    String userAgent = "APN/1.0 Azure Blob FS/3.4.0-SNAPSHOT (PrivateBuild "
        + "JavaJRE 1.8.0_252; Linux 5.3.0-59-generic/amd64; openssl-1.0; "
        + "UNKNOWN/UNKNOWN) MSFT";
    client = TestAbfsClient.setAbfsClientField(client, "userAgent", userAgent);

    return client;
  }

  private static AbfsClient setAbfsClientField(
      final AbfsClient client,
      final String fieldName,
      Object fieldObject) throws Exception {

    Field field = AbfsClient.class.getDeclaredField(fieldName);
    field.setAccessible(true);
    Field modifiersField = Field.class.getDeclaredField("modifiers");
    modifiersField.setAccessible(true);
    modifiersField.setInt(field,
        field.getModifiers() & ~java.lang.reflect.Modifier.FINAL);
    field.set(client, fieldObject);
    return client;
  }

  /**
   * Test helper method to access private createRequestUrl method.
   * @param client test AbfsClient instace
   * @param path path to generate Url
   * @return return store path url
   * @throws AzureBlobFileSystemException
   */
  public static URL getTestUrl(AbfsClient client, String path) throws
      AzureBlobFileSystemException {
    final AbfsUriQueryBuilder abfsUriQueryBuilder
        = client.createDefaultUriQueryBuilder();
    return client.createRequestUrl(path, abfsUriQueryBuilder.toString());
  }

  /**
   * Test helper method to access private createDefaultHeaders method.
   * @param client test AbfsClient instance
   * @return List of AbfsHttpHeaders
   */
  public static List<AbfsHttpHeader> getTestRequestHeaders(AbfsClient client) {
    return client.createDefaultHeaders();
  }

  /**
   * Test helper method to create an AbfsRestOperation instance.
   * @param type RestOpType
   * @param client AbfsClient
   * @param method HttpMethod
   * @param url Test path url
   * @param requestHeaders request headers
   * @return instance of AbfsRestOperation
   */
  public static AbfsRestOperation getRestOp(AbfsRestOperationType type,
      AbfsClient client,
      String method,
      URL url,
      List<AbfsHttpHeader> requestHeaders) {
    return new AbfsRestOperation(
        type,
        client,
        method,
        url,
        requestHeaders);
>>>>>>> CHANGE (74cfc9 WIP: Allow for custom shared key signer for ABFS)
  }
}
