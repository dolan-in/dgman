/*
 * Copyright (C) 2025 Dolan and Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package dgman

import (
	"sync"

	"github.com/go-logr/logr"
	"github.com/go-logr/stdr"
)

var (
	logrLogger logr.Logger = stdr.New(nil)
	logrOnce   sync.Once
)

// SetLogger sets the global logr.Logger for dgman
func SetLogger(l logr.Logger) {
	logrLogger = l
}

// Logger returns the current global logr.Logger
func Logger() logr.Logger {
	logrOnce.Do(func() {
		if logrLogger.GetSink() == nil {
			// Default to stdout logger if not set
			logrLogger = stdr.New(nil)
		}
	})
	return logrLogger
}

// init ensures the default logger is set to stdout
func init() {
	SetLogger(stdr.New(nil))
}
