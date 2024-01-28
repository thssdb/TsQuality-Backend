package cn.edu.tsinghua.tsquality.mappers.objects;

public interface Mapper<A, B> {
  B mapTo(A a);

  A mapFrom(B b);
}
