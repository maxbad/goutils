package quit

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

var (
	gQuitEvent *QuitEvent
	once       sync.Once
)

// init
func init() {
	gQuitEvent = NewQuitEvent()
}

// GetQuitEvent get singleton quit event
func GetQuitEvent() *QuitEvent {
	once.Do(func() {
		if gQuitEvent == nil {
			gQuitEvent = NewQuitEvent()
		}
	})
	return gQuitEvent
}

// QuitEvent quit event struct
type QuitEvent struct {
	*Event
	// quit closer list to be close
	quitCloserList []QuitCloser
	// io closer list to be close
	closerList []io.Closer
	// stop func list
	stopFuncList []func()
	// counts active goroutines for GracefulStop
	serveWG sync.WaitGroup
}

// QuitCloser Shutdown
type QuitCloser interface {
	// Once Shutdown has been called on a server, it may not be reused;
	// future calls to methods such as Serve will return ErrServerClosed.
	Shutdown(ctx context.Context) error
}

// NewQuitEvent returns a new, ready-to-use Event.
func NewQuitEvent() *QuitEvent {
	return &QuitEvent{
		Event: NewEvent(),
	}
}

// AddGoroutine Incr count of running goroutine
func (q *QuitEvent) AddGoroutine() {
	q.serveWG.Add(1)
}

// DoneGoroutine Decr count of running goroutine
func (q *QuitEvent) DoneGoroutine() {
	q.serveWG.Done()
}

// WaitGoroutines Waiting all running goroutine quit.
func (q *QuitEvent) WaitGoroutines() {
	q.serveWG.Wait()
}

// RegisterQuitCloser closer will be called before goroutine quit.
func (q *QuitEvent) RegisterQuitCloser(closer QuitCloser) {
	q.quitCloserList = append(q.quitCloserList, closer)
}

// RegisterCloser closer will be called before goroutine quit.
func (q *QuitEvent) RegisterCloser(closer io.Closer) {
	q.closerList = append(q.closerList, closer)
}

// RegisterStopFunc stop func will be called before goroutine quit.
func (q *QuitEvent) RegisterStopFunc(stopFunc func()) {
	q.stopFuncList = append(q.stopFuncList, stopFunc)
}

// GracefulStop Graceful stop all running goroutines.
func (q *QuitEvent) GracefulStop() {
	q.Fire()
	for _, closer := range q.quitCloserList {
		if closer != nil {
			_ = closer.Shutdown(context.TODO())
		}
	}
	for _, closer := range q.closerList {
		if closer != nil {
			_ = closer.Close()
		}
	}
	for _, stopFunc := range q.stopFuncList {
		if stopFunc != nil {
			stopFunc()
		}
	}
	q.WaitGoroutines()
}

// WaitSignal stop signal handle
func WaitSignal(waitSecond int) {
	shutdownHook := make(chan os.Signal, 1)
	signal.Notify(shutdownHook,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT,
		os.Interrupt)
	sig := <-shutdownHook

	fmt.Printf("caught sig exit sig:%v\n", sig)
	go func() {
		GetQuitEvent().GracefulStop()
	}()
	// wait 3 second for quit event graceful stop.
	time.Sleep(time.Duration(waitSecond) * time.Second)
	os.Exit(0)
}
