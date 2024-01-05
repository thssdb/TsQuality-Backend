package cn.edu.tsinghua.tsquality.model.entity;

import lombok.Data;

@Data
public class IoTDBChunk {
  public int cid;
  public int fid;
  public int sid;
  public long offset;

  public IoTDBChunk(int fid, int sid, long offset) {
    this.fid = fid;
    this.sid = sid;
    this.offset = offset;
  }
}
