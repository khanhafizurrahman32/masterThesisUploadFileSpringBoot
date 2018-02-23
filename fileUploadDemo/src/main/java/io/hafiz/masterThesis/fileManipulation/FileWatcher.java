package io.hafiz.masterThesis.fileManipulation;

import java.io.File;
import java.util.TimerTask;

public abstract class FileWatcher extends TimerTask {

	private long timeStamp;
	private File file;
	
	
	public FileWatcher(File file) {
		super();
		this.timeStamp = file.lastModified();
		this.file = file;
	}


	@Override
	public void run() {
		long timeStamp = file.lastModified();
		
		if(this.timeStamp != timeStamp) {
			this.timeStamp = timeStamp;
			onChange(file);
		}

	}


	protected abstract void onChange(File file) ;

}
