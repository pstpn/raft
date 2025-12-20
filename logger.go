package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

type SimpleLogger struct {
	z            zerolog.Logger
	getStateFunc func() string
}

func NewSimpleLogger(level string) *SimpleLogger {
	logLevel, err := zerolog.ParseLevel(level)
	if err != nil {
		logLevel = zerolog.InfoLevel
	}

	zerolog.SetGlobalLevel(logLevel)

	logger := &SimpleLogger{}

	writer := zerolog.ConsoleWriter{
		Out:        os.Stderr,
		TimeFormat: "2006-01-02 15:04:05",
		FormatLevel: func(i any) string {
			level := strings.ToUpper(fmt.Sprintf("%s", i))
			switch {
			case strings.Contains(level, "DEBUG"):
				return "\033[36m" + fmt.Sprintf("%-5s", level) + "\033[0m"
			case strings.Contains(level, "INFO"):
				return "\033[32m" + fmt.Sprintf("%-5s", level) + "\033[0m"
			case strings.Contains(level, "WARN"):
				return "\033[33m" + fmt.Sprintf("%-5s", level) + "\033[0m"
			case strings.Contains(level, "ERROR"):
				return "\033[31m" + fmt.Sprintf("%-5s", level) + "\033[0m"
			default:
				return fmt.Sprintf("%-5s", level)
			}
		},
		FormatMessage: func(i any) string {
			if logger.getStateFunc != nil {
				state := logger.getStateFunc()
				return colorizeState(state) + " " + fmt.Sprint(i)
			}
			return fmt.Sprint(i)
		},
		FormatFieldName: func(i any) string {
			return fmt.Sprintf("%s=", i)
		},
		FormatFieldValue: func(i any) string {
			return fmt.Sprint(i)
		},
	}

	log.Logger = log.Output(writer).With().Timestamp().Logger()
	logger.z = log.Logger

	return logger
}

func (l *SimpleLogger) SetStateFunc(fn func() string) {
	l.getStateFunc = fn
}

func colorizeState(state string) string {
	switch state {
	case "Follower":
		return "\033[94m[F]\033[0m"
	case "Candidate":
		return "\033[93m[C]\033[0m"
	case "Leader":
		return "\033[95m[L]\033[0m"
	default:
		return fmt.Sprintf("[%s]", state)
	}
}

func (l *SimpleLogger) Debug(message string) { l.z.Debug().Msg(message) }

func (l *SimpleLogger) Debugf(format string, args ...any) { l.z.Debug().Msgf(format, args...) }

func (l *SimpleLogger) Info(message string) { l.z.Info().Msg(message) }

func (l *SimpleLogger) Infof(format string, args ...any) { l.z.Info().Msgf(format, args...) }

func (l *SimpleLogger) Warn(message string) { l.z.Warn().Msg(message) }

func (l *SimpleLogger) Warnf(format string, args ...any) { l.z.Warn().Msgf(format, args...) }

func (l *SimpleLogger) Error(message string) { l.z.Error().Msg(message) }

func (l *SimpleLogger) Errorf(format string, args ...any) { l.z.Error().Msgf(format, args...) }
