package org.apache.hadoop.fs.azurebfs.services;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

import java.util.ArrayList;

public class AbfsListPathResponse {

  private Path path;
  private String continuation;
  private ArrayList<FileStatus> fileStatuses;

  public AbfsListPathResponse(Path path, ArrayList<FileStatus> fileStatuses, String continuation) {
    this.path = path;
    this.fileStatuses = fileStatuses;
    this.continuation = continuation;
  }

  public Path getPath() {
    return path;
  }

  public ArrayList<FileStatus> getFileStatuses() {
    return fileStatuses;
  }

  public String getContinuation() {
    return continuation;
  }

  public boolean shouldLoadNextBatch(int currIndex) {
    return currIndex == fileStatuses.size()
        && continuation != null
        && !continuation.isEmpty();
  }
}
