package termui

import (
	"io"
	stdlog "log"
	"os"
	"runtime"
	"sync"

	"github.com/pterm/pterm"
	"github.com/rs/zerolog"
	zlog "github.com/rs/zerolog/log"
)

type lockedWriter struct {
	mx sync.Mutex
	w  io.Writer
}

func (l *lockedWriter) Write(p []byte) (int, error) {
	l.mx.Lock()
	defer l.mx.Unlock()
	return l.w.Write(p)
}

var output = &lockedWriter{w: os.Stdout}

func Output() io.Writer {
	return output
}

func UseLivePrinters() bool {
	return runtime.GOOS != "windows"
}

func ConfigurePTerm() {
	pterm.SetDefaultOutput(output)

	pterm.Info.Writer = output
	pterm.Warning.Writer = output
	pterm.Success.Writer = output
	pterm.Error.Writer = output
	pterm.Fatal.Writer = output
	pterm.Debug.Writer = output
	pterm.Description.Writer = output

	pterm.DefaultBasicText.Writer = output
	pterm.DefaultBox.Writer = output
	pterm.DefaultTable.Writer = output
	pterm.DefaultSpinner.Writer = output
	pterm.DefaultProgressbar.Writer = output
}

func ConfigureStdLogger() {
	stdlog.SetOutput(output)
}

func SetZerologLevel(level zerolog.Level) {
	zlog.Logger = zlog.Output(zerolog.ConsoleWriter{Out: output}).Level(level)
}

type Spinner struct {
	live *pterm.SpinnerPrinter
}

func StartSpinner(text ...any) (*Spinner, error) {
	if !UseLivePrinters() {
		pterm.Info.Println(text...)
		return &Spinner{}, nil
	}

	sp, err := pterm.DefaultSpinner.Start(text...)
	if err != nil {
		return nil, err
	}
	return &Spinner{live: sp}, nil
}

func (s *Spinner) Success(message ...any) {
	if s == nil {
		return
	}
	if s.live != nil {
		s.live.Success(message...)
		return
	}
	if len(message) > 0 {
		pterm.Success.Println(message...)
	}
}

func (s *Spinner) Fail(message ...any) {
	if s == nil {
		return
	}
	if s.live != nil {
		s.live.Fail(message...)
		return
	}
	if len(message) > 0 {
		pterm.Error.Println(message...)
	}
}

func (s *Spinner) Warning(message ...any) {
	if s == nil {
		return
	}
	if s.live != nil {
		s.live.Warning(message...)
		return
	}
	if len(message) > 0 {
		pterm.Warning.Println(message...)
	}
}

func (s *Spinner) Stop() error {
	if s == nil || s.live == nil {
		return nil
	}
	return s.live.Stop()
}

type ProgressBar struct {
	live *pterm.ProgressbarPrinter
}

func StartProgressbar(total int, title string) (*ProgressBar, error) {
	if !UseLivePrinters() {
		pterm.Info.Println(title)
		return &ProgressBar{}, nil
	}

	pr, err := pterm.DefaultProgressbar.WithTotal(total).WithTitle(title).Start()
	if err != nil {
		return nil, err
	}
	return &ProgressBar{live: pr}, nil
}

func (p *ProgressBar) Increment() {
	if p == nil || p.live == nil {
		return
	}
	p.live.Increment()
}

func (p *ProgressBar) Stop() error {
	if p == nil || p.live == nil {
		return nil
	}
	_, err := p.live.Stop()
	return err
}
