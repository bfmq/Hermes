package main

import (
	"github.com/donnie4w/go-logger/logger"
)

func init() {
	logger.SetConsole(true)
	logger.SetLevel(logger.DEBUG)
	logger.SetRollingDaily("../logs", "run.log")
}
