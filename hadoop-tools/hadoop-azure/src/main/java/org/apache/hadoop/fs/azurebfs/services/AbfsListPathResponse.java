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


import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

import java.util.ArrayList;


/**
 * Abfslist path response used for batch listing mainly.
 */
public class AbfsListPathResponse {

  private Path path;
  private String continuation;
  private ArrayList<FileStatus> fileStatuses;

  public ArrayList<FileStatus> getFileStatuses() {
    return fileStatuses;
  }

  public void setFileStatuses(ArrayList<FileStatus> fileStatuses) {
    this.fileStatuses = fileStatuses;
  }

  public AbfsListPathResponse(Path path, ArrayList<FileStatus> fileStatuses, String continuation) {
    this.path=path;
    this.fileStatuses=fileStatuses;
    this.continuation = continuation;
  }


  public Path getPath() {
    return path;
  }

  public void setPath(Path path) {
    this.path = path;
  }

  public String getContinuation() {
    return continuation;
  }

  public void setContinuation(String continuation) {
    this.continuation = continuation;
  }

  public  boolean shouldLoadNextBatch(int currIndex){

    if (currIndex == (fileStatuses.size()) && continuation != null && !continuation.isEmpty()) {
      return true;
    }
    return false;
  }
}
