package com.onlinelog.analysis;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;

public class StderrReader extends Thread {
    private static final Logger logger = LoggerFactory.getLogger(StderrReader.class);
    private BufferedReader input;
    private boolean logStderr;

    protected StderrReader(BufferedReader input, boolean logStderr) {
      this.input = input;
      this.logStderr = logStderr;
    }

    @Override
    public void run() {
      try {
        int i = 0;
        String line = null;
        while ((line = input.readLine()) != null) {
          if (logStderr) {
            // There is no need to read 'line' with a charset
            // as we do not to propagate it.
            // It is in UTF-16 and would be printed in UTF-8 format.
            logger.info("StderrLogger[{}] = '{}'", ++i, line);
          }
        }
      } catch (IOException e) {
        logger.info("StderrLogger exiting", e);
      } finally {
        try {
          if (input != null) {
            input.close();
          }
        } catch (IOException ex) {
          logger.error("Failed to close stderr reader for exec source", ex);
        }
      }
    }
  }