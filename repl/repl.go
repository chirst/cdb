// repl (read eval print loop) adapts db to the command line.
package repl

import (
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"slices"
	"strings"
	"syscall"

	"github.com/chirst/cdb/db"
	"golang.org/x/term"
)

const (
	// emptyRowValue is printed when the cell in a row is nil.
	emptyRowValue = "NULL"
	// emptyHeaderValue is printed when the cell in a header is the empty string
	emptyHeaderValue = "<anonymous>"
	// prompt is the prompt.
	prompt = "cdb> "
	// promptContinued is the prompt when it is pending termination for example
	// by a semi colon.
	promptContinued = "...> "
)

type repl struct {
	db       *db.DB
	terminal *term.Terminal
}

func New(db *db.DB) *repl {
	r := &repl{
		db:       db,
		terminal: term.NewTerminal(os.Stdin, prompt),
	}
	r.loadHistory()
	return r
}

func (r *repl) Run() {
	r.writeLn("Welcome to cdb. Type .exit to exit")
	if r.db.UseMemory {
		r.writeWarning("WARN database is running in memory and will not persist changes")
	}

	// Handling kill signals works under two methods for the REPL. When the
	// terminal is in raw mode the signals are caught by readline as bytes. When
	// the terminal is not in raw mode the signals are caught by the following
	// channel.
	//
	// The handling keeping in mind two major considerations in that the
	// terminal history is written to and the database always allows a long
	// running query to be shut down.
	c := make(chan os.Signal, 2)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		r.exitGracefully()
	}()

	previousInput := ""
	for {
		line := r.readLine(previousInput)
		input := previousInput + line
		if len(input) == 0 {
			continue
		}
		if input[0] == '.' {
			if input == ".exit" {
				r.exitGracefully()
			}
			r.writeLn("Command not supported")
			continue
		}

		statements := r.db.Tokenize(input)
		terminated := r.db.IsTerminated(statements)
		if !terminated {
			previousInput = input + "\n"
			continue
		}
		previousInput = ""
		for _, statement := range statements {
			result := r.db.Execute(statement, []any{})
			if result.Err != nil {
				r.writeLn("Err: " + result.Err.Error())
				continue
			}
			if result.Text != "" {
				r.writeLn(result.Text)
			}
			if len(result.ResultRows) != 0 {
				r.writeLn(r.printRows(result.ResultHeader, result.ResultRows))
			}
			r.writeLn("Time: " + result.Duration.String())
		}
	}
}

func (r *repl) readLine(previousInput string) string {
	oldState, err := term.MakeRaw(int(os.Stdin.Fd()))
	if err != nil {
		panic(err)
	}
	defer term.Restore(int(os.Stdin.Fd()), oldState)
	if previousInput == "" {
		r.terminal.SetPrompt(prompt)
	} else {
		r.terminal.SetPrompt(promptContinued)
	}
	line, err := r.terminal.ReadLine()
	if err != nil {
		if err == io.EOF {
			term.Restore(int(os.Stdin.Fd()), oldState)
			r.exitGracefully()
		}
		panic("err reading line: " + err.Error())
	}
	return line
}

func (r *repl) writeLn(text string) {
	r.terminal.Write(([]byte)(text + "\n"))
}

func (r *repl) writeWarning(text string) {
	r.terminal.Write(r.terminal.Escape.Yellow)
	r.writeLn(text)
	r.terminal.Write(r.terminal.Escape.Reset)
}

func (r *repl) printRows(resultHeader []string, resultRows [][]*string) string {
	ret := ""
	widths := r.getWidths(resultHeader, resultRows)
	ret += r.printHeader(resultHeader, widths)
	ret = ret + "\n"
	for _, row := range resultRows {
		ret += r.printRow(row, widths)
		ret = ret + "\n"
	}
	if len(resultRows) == 0 {
		ret = ret + "(0 rows)\n"
	}
	return ret
}

func (*repl) getWidths(header []string, rows [][]*string) []int {
	widths := make([]int, len(rows[0]))
	for i := range widths {
		widths[i] = 0
	}
	for i, hCol := range header {
		size := len(emptyHeaderValue)
		if hCol != "" {
			size = len(hCol)
		}
		if widths[i] < size {
			widths[i] = size
		}
	}
	for _, row := range rows {
		for i, column := range row {
			size := len(emptyRowValue)
			if column != nil {
				size = len(*column)
			}
			if widths[i] < size {
				widths[i] = size
			}
		}
	}
	return widths
}

func (*repl) printHeader(row []string, widths []int) string {
	ret := ""
	for i, column := range row {
		v := emptyHeaderValue
		if column != "" {
			v = column
		}
		ret = ret + fmt.Sprintf(" %-*s ", widths[i], v)
		if i != len(row)-1 {
			ret = ret + "|"
		}
	}
	ret = ret + "\n"
	for i := range row {
		ret = ret + fmt.Sprintf("-%s-", strings.Repeat("-", widths[i]))
		if i != len(row)-1 {
			ret = ret + "+"
		}
	}
	return ret
}

func (*repl) printRow(row []*string, widths []int) string {
	ret := ""
	for i, column := range row {
		v := emptyRowValue
		if column != nil {
			v = *column
		}
		ret = ret + fmt.Sprintf(" %-*s ", widths[i], v)
		if i != len(row)-1 {
			ret = ret + "|"
		}
	}
	return ret
}

func (r *repl) exitGracefully() {
	r.saveHistory()
	os.Exit(0)
}

func (r *repl) loadHistory() {
	p, err := r.getHistoryPath()
	if err != nil {
		r.writeWarning("failed to get history path " + err.Error())
		return
	}
	contents, err := os.ReadFile(p)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return
		}
		r.writeWarning("failed to load history " + err.Error())
		return
	}
	lines := strings.Split((string)(contents), "\n")
	slices.Reverse(lines)
	for _, line := range lines {
		if line == "" {
			continue
		}
		r.terminal.History.Add(line)
	}
}

func (r *repl) saveHistory() {
	history := []byte{}
	for i := range r.terminal.History.Len() {
		str_entry := r.terminal.History.At(i)
		byte_entry := ([]byte)(str_entry + "\n")
		history = append(history, byte_entry...)
	}
	p, err := r.getHistoryPath()
	if err != nil {
		r.writeWarning("failed to get history path for saving " + err.Error())
		return
	}
	f, err := os.OpenFile(p, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		r.writeWarning("failed to open history file for saving " + err.Error())
		return
	}
	defer f.Close()
	err = f.Truncate(0)
	if err != nil {
		r.writeWarning("failed to overwrite history " + err.Error())
		return
	}
	_, err = f.Write(history)
	if err != nil {
		r.writeWarning("failed to write history " + err.Error())
		return
	}
}

func (r *repl) getHistoryPath() (string, error) {
	dir, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	return dir + "/.cdb_history", nil
}
