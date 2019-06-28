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

package org.apache.hadoop.fs.azure;

import java.io.IOException;
import java.net.URISyntaxException;

import org.apache.hadoop.fs.azurebfs.oauth2.AzureADAuthenticator;
import org.apache.hadoop.fs.azurebfs.oauth2.AzureADToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.microsoft.azure.storage.StorageCredentialsToken;
import com.microsoft.azure.storage.StorageException;

/***
 * An implementation of the StorageInterface for SAS Key mode.
 *
 */

public class OAuthStorageInterfaceImpl extends StorageInterfaceImpl {
    public static final Logger LOG = LoggerFactory.getLogger(
            OAuthStorageInterfaceImpl.class);
    private AzureADCredentials adCredentials;
    private AzureADToken token;

    @Override
    public StorageInterface.CloudBlobContainerWrapper getContainerReference(String uri)
            throws URISyntaxException, StorageException {
        synchronized(this) {
            StorageCredentialsToken credentials = (StorageCredentialsToken) serviceClient.getCredentials();
            assert credentials.getToken().equals(token.getAccessToken());
            if(tokenAboutToExpire()) {
                try {
                    updateTokenAndClient();
                } catch(IOException ex) {
                    StorageException.translateClientException(ex);
                }
            }
        }
        return super.getContainerReference(uri);
    }

    public synchronized void updateTokenAndClient() throws IOException{
        //Update token
        this.token = AzureADAuthenticator.getTokenUsingClientCreds(adCredentials.getClientId(),
                adCredentials.getTokenEndpoint(), adCredentials.getClientSecret());
        //Update client credentials
        this.serviceClient.setCloudBlobClientCredentials(
                new StorageCredentialsToken(getCredentials().getAccountName(),
                        this.token.getAccessToken()));
    }

    public synchronized boolean tokenAboutToExpire() {
        if(AzureADAuthenticator.isTokenAboutToExpire(this.token)) {
            return true;
        }
        return false;
    }

    public synchronized String updateToken() throws IOException {
        this.token = AzureADAuthenticator.getTokenUsingClientCreds(adCredentials.getClientId(),
                adCredentials.getTokenEndpoint(), adCredentials.getClientSecret());
        return this.token.getAccessToken();
    }

    public void setAdCredentials(AzureADCredentials adCredentials) {
        this.adCredentials = adCredentials;
    }

    public void setToken(AzureADToken token) {
        this.token = token;
    }
}