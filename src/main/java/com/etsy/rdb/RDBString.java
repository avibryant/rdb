package com.etsy.rdb;

import java.io.*;
import java.util.Arrays;
import com.ning.compress.lzf.LZFEncoder;

public abstract class RDBString {
  abstract void write(RDBOutputStream out) throws IOException;

  public static RDBString uncompressed(byte[] bytes) {
    return new RDBByteString(bytes);
  }

  public static RDBString create(byte[] bytes) throws IOException {
//compression doesn't seem to work yet
    if(bytes.length < 20)
      return uncompressed(bytes);
    else
      return new RDBCompressedString(bytes);
  }

  public static RDBString create(String string) throws IOException {
    return create(string.getBytes());
  }

  public static RDBString create(int i) {
    return new RDBIntString(i);
  }
}

class RDBByteString extends RDBString {
  byte[] bytes;

  RDBByteString(byte[] bytes) {
    this.bytes = bytes;
  }

  void write(RDBOutputStream out) throws IOException {
    out.writeLength(bytes.length);
    out.write(bytes);
  }
}

class RDBCompressedString extends RDBByteString {
  byte[] compressed;

  RDBCompressedString(byte[] bytes) throws IOException {
    super(bytes);
    byte[] compressedWithHeaders = LZFEncoder.encode(bytes);
    compressed = Arrays.copyOfRange(compressedWithHeaders, 7, compressedWithHeaders.length - 1);
    compressed[0] = 1;

    System.out.println("Compressed " + bytes.length + " to " + compressed.length);
  }

  void write(RDBOutputStream out) throws IOException {
    if((compressed.length + 4) >= bytes.length)
      super.write(out);
    else {
      out.write(0xC3);
      out.writeLength(compressed.length);
      out.writeLength(bytes.length);
      out.write(compressed);
    }
  }
}

class RDBIntString extends RDBString {
  int value;

  RDBIntString(int value) {
    this.value = value;
  }

  void write(RDBOutputStream out) throws IOException {
    if (value >= -(1<<7) && value <= (1<<7)-1) {
      out.write(0xC0);
      out.write(value&0xFF);
    } else if (value >= -(1<<15) && value <= (1<<15)-1) {
      out.write(0xC1);
      out.write(value&0xFF);
      out.write((value>>8)&0xFF);
    } else {
      out.write(0xC2);
      out.write(value&0xFF);
      out.write((value>>8)&0xFF);
      out.write((value>>16)&0xFF);
      out.write((value>>24)&0xFF);
    }
  }
}