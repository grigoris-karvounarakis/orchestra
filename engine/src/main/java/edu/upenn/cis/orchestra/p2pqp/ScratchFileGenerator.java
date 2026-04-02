package edu.upenn.cis.orchestra.p2pqp;

import java.io.File;

public interface ScratchFileGenerator {
	File getFile();
	void cleanup();
}
