package cn.edu.tsinghua.tsquality.model.entity;

import lombok.Data;

@Data
public class IoTDBPage {
  public int pid;
  public int cid;
  public int sid;
  public long pageIndex;

  public IoTDBPage(int cid, int sid, long pageIndex) {
    this.cid = cid;
    this.sid = sid;
    this.pageIndex = pageIndex;
  }
}
