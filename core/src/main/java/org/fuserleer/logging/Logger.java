package org.fuserleer.logging;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Date;
import java.util.concurrent.LinkedBlockingQueue;

public final class Logger implements AutoCloseable
{
	private PrintWriter	filePrintWriter;
	private String 		name;
	private String 		filename;
	private int 		levels;
	private boolean		stdOut;
	private final LinkedBlockingQueue<String> logEntries = new LinkedBlockingQueue<>();

	Logger(String name, String filename, int levels, boolean stdOut)
	{
		this.name = name;
		this.levels = levels;
		this.stdOut = stdOut;
		this.filename = filename;

		if (filename != null)
		{
			File file = new File(filename);

			if (file.exists() == false)
				file.getParentFile().mkdirs();

			try
			{
				this.filePrintWriter = new PrintWriter(file);
			} catch (FileNotFoundException e) {}
		}
	}

	synchronized void flush()
	{
		if (this.filePrintWriter != null)
		{
			if (this.logEntries.isEmpty() == false)
			{
				String logEntry = null;

				while ((logEntry = this.logEntries.poll()) != null)
					this.filePrintWriter.println(logEntry);

				this.filePrintWriter.flush();
			}
		}
	}

	@Override
	public void close()
	{
		if (this.filePrintWriter != null)
		{
			flush();
			this.filePrintWriter.close();
		}
	}

	public void info(String message) { info(message, null); }
	public void info(Throwable ex) { info(null, ex); }
	public void info(String message, Throwable ex) { log(Logging.INFO, message, ex); }

	public void error(String message) { error(message, null); }
	public void error(Throwable ex) { error(null, ex); }
	public void error(String message, Throwable ex) { log(Logging.ERROR, message, ex); }

	public void debug(String message) { debug(message, null); }
	public void debug(Throwable ex) { debug(null, ex); }
	public void debug(String message, Throwable ex) { log(Logging.DEBUG, message, ex); }

	public void warn(String message) { warn(message, null); }
	public void warn(Throwable ex) { warn(null, ex); }
	public void warn(String message, Throwable ex) { log(Logging.WARN, message, ex); }

	public void trace(String message) { trace(message, null); }
	public void trace(Throwable ex) { trace(null, ex); }
	public void trace(String message, Throwable ex) { log(Logging.TRACE, message, ex); }

	public void fatal(String message) { fatal(message, null); }
	public void fatal(Throwable ex) { fatal(null, ex); }
	public void fatal(String message, Throwable ex) { log(Logging.FATAL, message, ex); }

	private void log(int level, String message, Throwable ex)
	{
		String levelString = null;

		switch (this.levels&level)
		{
			case Logging.FATAL:
				levelString = " FATAL: ";
				break;
			case Logging.ERROR:
				levelString = " ERROR: ";
				break;
			case Logging.WARN:
				levelString = " WARN: ";
				break;
			case Logging.INFO:
				levelString = " INFO: ";
				break;
			case Logging.DEBUG:
				levelString = " DEBUG: ";
				break;
			case Logging.TRACE:
				levelString = " TRACE: ";
				break;
			default:
				return;
		}

		StringBuilder builder = new StringBuilder(256);
		long time = new Date().getTime();
		long midnight = time - ((time / (86400 * 1000))*(86400 * 1000));
		long hours = midnight / (3600 * 1000);
		long minutes = time / (1000 * 60) % 60;
		long seconds = time / (1000) % 60;
		long milliseconds = time % 1000;

		if (hours <= 9)
			builder.append('0');
		builder.append(hours);
		builder.append(':');

		if (minutes <= 9)
			builder.append('0');
		builder.append(minutes);
		builder.append(':');

		if (seconds <= 9)
			builder.append('0');
		builder.append(seconds);
		builder.append(',');

		if (milliseconds <= 99)
			builder.append('0');
		if (milliseconds <= 9)
			builder.append('0');
		builder.append(milliseconds);

		builder.append(levelString);

		if (this.name != null)
		{
			builder.append('[');
			builder.append(name);
			builder.append("] ");
		}

		StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();

		int stackIndex = 0;
		for ( ; stackIndex < stackTrace.length ; stackIndex ++)
		{
			if (!stackTrace[stackIndex].getClassName().equals(this.getClass().getCanonicalName()) && !stackTrace[stackIndex].getClassName().equals(Thread.class.getCanonicalName()))
				break;
		}

		String fullClassName = stackTrace[stackIndex].getClassName();
	    String className = fullClassName.substring(fullClassName.lastIndexOf(".") + 1);
	    int lineNumber = stackTrace[stackIndex].getLineNumber();

	    builder.append(className);
	    builder.append(':');
	    builder.append(lineNumber);
	    builder.append(" - ");

	    if (message != null)
	    	builder.append(message);

		if (ex != null)
		{
			StringWriter writer = new StringWriter(256);
			ex.printStackTrace(new PrintWriter(writer));
	    	builder.append(System.lineSeparator());
			builder.append(writer.toString().trim());
		}

		String logEntry = builder.toString();

		if (this.filePrintWriter != null)
			this.logEntries.add(logEntry);

		if (this.stdOut)
			Logging.getInstance().toStdOut(logEntry);
	}

	public int getLevels() { return this.levels; }
	public void setLevels(int levels) { this.levels = levels; }
	public boolean hasLevel(int level) { return (this.levels & level) == level; }

	public String getName() { return this.name; }

	public String getFilename() { return this.filename; }

	public boolean isStdOut() { return this.stdOut; }
	public void setStdOut(boolean stdOut) { this.stdOut = stdOut; }
}
