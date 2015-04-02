package logging

type SimpleLogger interface {
	Debugf(format string, v ...interface{})
	Infof(format string, v ...interface{})
	Warnf(format string, v ...interface{})
	Errorf(format string, v ...interface{})
}

//--------------------------------------------------------------------
// NilLogger - SimpleLogger that discards all Writes
//--------------------------------------------------------------------
type nilLogger struct{}

var _ SimpleLogger = &nilLogger{}

func (l *nilLogger) Debugf(format string, v ...interface{}) {}
func (l *nilLogger) Infof(format string, v ...interface{})  {}
func (l *nilLogger) Warnf(format string, v ...interface{})  {}
func (l *nilLogger) Errorf(format string, v ...interface{}) {}

// NilLogger returns an instance of the NilLogger, which simply discards all writes.
func NewNilLogger() SimpleLogger {
	return &nilLogger{}
}

//--------------------------------------------------------------------
// LoggerWithPrefix - SimpleLogger with prefix
//--------------------------------------------------------------------
type LoggerWithPrefix struct {
	prefix        string
	WrappedLogger SimpleLogger
}

var _ SimpleLogger = &LoggerWithPrefix{}

// LoggerWithPrefix returns an instance of the LoggerWithPrefix, which wraps the given logger,
// and adds the given prefix to each message.
// Note: it's not safe for the prefix string to contain the '%' character, as it will interfere with the fmt parsing of messages.
func NewLoggerWithPrefix(prefix string, logger SimpleLogger) *LoggerWithPrefix {
	if loggerWithPrefix, ok := logger.(*LoggerWithPrefix); ok {
		return loggerWithPrefix.WithAnotherPrefix(prefix)
	} else {
		return &LoggerWithPrefix{
			prefix:        prefix,
			WrappedLogger: logger,
		}
	}
}

func (l *LoggerWithPrefix) Debugf(format string, v ...interface{}) {
	l.WrappedLogger.Debugf(l.joinPrefix(format), v...)
}
func (l *LoggerWithPrefix) Infof(format string, v ...interface{}) {
	l.WrappedLogger.Infof(l.joinPrefix(format), v...)
}
func (l *LoggerWithPrefix) Warnf(format string, v ...interface{}) {
	l.WrappedLogger.Warnf(l.joinPrefix(format), v...)
}
func (l *LoggerWithPrefix) Errorf(format string, v ...interface{}) {
	l.WrappedLogger.Errorf(l.joinPrefix(format), v...)
}

// WithAnotherPrefix returns a new LoggerWithPrefix that keeps the same wrapped logger, but concatenates another prefix
func (l *LoggerWithPrefix) WithAnotherPrefix(prefix string) *LoggerWithPrefix {
	return &LoggerWithPrefix{
		prefix:        l.prefix + prefix,
		WrappedLogger: l.WrappedLogger,
	}
}

func (l *LoggerWithPrefix) joinPrefix(s string) string {
	return l.prefix + " " + s
}
