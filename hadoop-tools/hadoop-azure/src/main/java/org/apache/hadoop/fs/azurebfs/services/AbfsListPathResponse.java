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