package edu.upenn.cis.orchestra.p2pqp;

import java.io.File;
import java.io.FilenameFilter;

public class SimpleScratchFileGenerator implements ScratchFileGenerator {
	public final File spillingDirectory;
	public final String filePrefix;
	
	public SimpleScratchFileGenerator(File spillingDirectory, String filePrefix) {
		this.spillingDirectory = spillingDirectory;
		this.filePrefix = filePrefix;
	}
	
	private int fileCount = 0;
	@Override
	public File getFile() {

		int number;
		synchronized (this) {
			number = fileCount++;
		}
		File retval = new File(spillingDirectory, filePrefix + "-" + Integer.toHexString(number));
		if (retval.exists()) {
			throw new IllegalStateException("Generated temporary file already exists: " + retval);
		}
		
		return retval;
	}
	@Override
	public void cleanup() {
		for (File f : spillingDirectory.listFiles(new FilenameFilter() {

			@Override
			public boolean accept(File arg0, String arg1) {
				return arg1.startsWith(filePrefix);
			}
			
		})) {
			f.delete();
		}
	}

}
