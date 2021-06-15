package org.apache.hadoop.fs.azurebfs.services;


import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;


/**
 * Abfslist path response used for batch listing mainly.
 */
public class AbfsListPathResponse {

  private Path path;
  private FileStatus [] fileStatuses ;
  private String continuation;

  public AbfsListPathResponse(Path path,FileStatus[] fileStatuses, String continuation) {
    this.path=path;
    this.fileStatuses = fileStatuses;
    this.continuation = continuation;
  }

  public Path getPath() {
    return path;
  }

  public void setPath(Path path) {
    this.path = path;
  }

  public FileStatus[] getFileStatuses() {
    return fileStatuses;
  }

  public void setFileStatuses(FileStatus[] fileStatuses) {
    this.fileStatuses = fileStatuses;
  }

  public String getContinuation() {
    return continuation;
  }

  public void setContinuation(String continuation) {
    this.continuation = continuation;
  }

  public  boolean shouldLoadNextBatch(int currIndex){

    if (currIndex == (fileStatuses.length) && continuation != null && !continuation.isEmpty()) {
      return true;
    }
    return false;
  }
}