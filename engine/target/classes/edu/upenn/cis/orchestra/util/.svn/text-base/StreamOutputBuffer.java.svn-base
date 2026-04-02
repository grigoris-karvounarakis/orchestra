package edu.upenn.cis.orchestra.util;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;

public class StreamOutputBuffer extends OutputBuffer {
	private DataOutputStream out;

	public StreamOutputBuffer(OutputStream os) {
		out = new DataOutputStream(os);
	}

	public void close() throws IOException {
		out.close();
	}
	
	@Override
	public void writeBoolean(boolean bool) {
		try {
			out.writeBoolean(bool);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void writeBytesNoLength(byte[] bytes) {
		try {
			out.write(bytes);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void writeBytesNoLength(byte[] bytes, int offset, int length) {
		try {
			out.write(bytes, offset, length);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void writeInt(int v) {
		try {
			out.writeInt(v);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void writeLong(long l) {
		try {
			out.writeLong(l);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void writeShort(short s) {
		try {
			out.writeShort(s);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public int size() {
		return out.size();
	}
}
