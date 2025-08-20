package logger

import (
	"fmt"
	"log"
	"os"
)

var currentLevel = LevelInfo

type Level int

const (
	LevelDebug Level = iota
	LevelInfo
	LevelWarn
	LevelError
)

func SetLevel(level string) {
	switch level {
	case "debug":
		currentLevel = LevelDebug
	case "info":
		currentLevel = LevelInfo
	case "warn":
		currentLevel = LevelWarn
	case "error":
		currentLevel = LevelError
	default:
		currentLevel = LevelInfo
	}
}

func Debug(format string, v ...interface{}) {
	if currentLevel <= LevelDebug {
		log.Printf("[DEBUG] "+format, v...)
	}
}

var errorLog *log.Logger

// Init sets up the file-based error logger.
func Init(logFile *os.File) {
	errorLog = log.New(logFile, "", log.LstdFlags)
}

// Info prints an info-level log to stdout.
func Info(format string, v ...interface{}) {
	log.Printf("[INFO] "+format, v...)
}

// Warn prints a warning to stdout.
func Warn(format string, v ...interface{}) {
	log.Printf("[WARN] "+format, v...)
}

// Error prints an error to stderr and logs it to the errorLog file.
func Error(format string, v ...interface{}) {
	msg := fmt.Sprintf("[ERROR] "+format, v...)
	fmt.Fprintln(os.Stderr, msg)
	if errorLog != nil {
		errorLog.Println(msg)
	}
}
