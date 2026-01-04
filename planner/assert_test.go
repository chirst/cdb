package planner

import (
	"errors"
	"fmt"
	"reflect"

	"github.com/chirst/cdb/vm"
)

// assertCommandsMatch is a helper for tests in the planner package.
func assertCommandsMatch(gotCommands, expectedCommands []vm.Command) error {
	didMatch := true
	errOutput := "\n"
	green := "\033[32m"
	red := "\033[31m"
	resetColor := "\033[0m"
	for i, c := range expectedCommands {
		if i >= len(gotCommands) {
			continue
		}
		color := green
		if !reflect.DeepEqual(c, gotCommands[i]) {
			didMatch = false
			color = red
		}
		errOutput += fmt.Sprintf(
			"%s%3d got  %#v%s\n    want %#v\n\n",
			color, i, gotCommands[i], resetColor, c,
		)
	}
	gl := len(gotCommands)
	wl := len(expectedCommands)
	if gl != wl {
		errOutput += red
		errOutput += fmt.Sprintf("got %d want %d commands\n", gl, wl)
		errOutput += resetColor
		didMatch = false
	}
	// This helper returns an error instead of making the assertion so a fatal
	// error will raise at the test site instead of the helper. This also allows
	// the caller to differentiate between a fatal or non fatal assertion.
	if !didMatch {
		return errors.New(errOutput)
	}
	return nil
}
