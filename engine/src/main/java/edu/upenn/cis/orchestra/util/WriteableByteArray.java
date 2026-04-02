package edu.upenn.cis.orchestra.util;

public interface WriteableByteArray {
	int getWriteableByteArrayOffset(int length, boolean writeLength);
	byte[] getWriteableByteArray();
}
