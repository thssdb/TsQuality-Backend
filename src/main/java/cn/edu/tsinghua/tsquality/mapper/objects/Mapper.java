package cn.edu.tsinghua.tsquality.mapper.objects;

public interface Mapper<A, B> {
  B mapTo(A a);

  A mapFrom(B b);
}
