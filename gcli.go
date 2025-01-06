package gcli

import (
	"context"
	"fmt"
	"github.com/budka-tech/envo"
	"github.com/budka-tech/iport"
	"github.com/budka-tech/logit-go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"net"
	"time"
)

type Params[T any] struct {
	Host          iport.Host
	ClientFactory func(*grpc.ClientConn) T
	Env           *envo.Env
	DialOptions   []grpc.DialOption
	Ports         *iport.Ports
	UseTLS        bool
	Timeout       time.Duration
	Logger        logit.Logger
}

type GCli[T any] struct {
	Con           T
	host          string
	listener      net.Listener
	conn          *grpc.ClientConn
	clientFactory func(*grpc.ClientConn) T
	dialOptions   []grpc.DialOption
	env           *envo.Env
	timeout       time.Duration
	logger        logit.Logger
}

func NewGClient[T any](params *Params[T]) *GCli[T] {
	if params.Timeout == 0 {
		params.Timeout = 1 * time.Minute
	}

	dialOptions := params.DialOptions
	if !params.UseTLS {
		dialOptions = append(dialOptions, grpc.WithInsecure())
	}

	host := params.Ports.FormatServiceTCP(params.Host)

	return &GCli[T]{
		host:          host,
		clientFactory: params.ClientFactory,
		dialOptions:   dialOptions,
		logger:        params.Logger,
		timeout:       params.Timeout,
	}
}

func (n *GCli[T]) Init(ctx context.Context) error {
	backoff := time.Second
	maxBackoff := time.Minute
	timeoutCtx, cancel := context.WithTimeout(ctx, n.timeout)
	defer cancel()

	for {
		select {
		case <-timeoutCtx.Done():
			return fmt.Errorf("превышено время ожидания при попытке подключения к %s: %w", n.host, timeoutCtx.Err())
		default:
			n.logger.Infof(ctx, "попытка подключения к сервису %s", n.host)

			conn, err := grpc.DialContext(timeoutCtx, n.host, n.dialOptions...)
			if err != nil {
				n.logger.Warnf(ctx, "не удалось подключиться к %s: %v. Повторная попытка через %v...", n.host, err, backoff)
				time.Sleep(backoff)
				backoff = min(backoff*2, maxBackoff)
				continue
			}

			n.conn = conn
			n.Con = n.clientFactory(conn)

			n.logger.Infof(ctx, "успешное подключение к сервису %s", n.host)

			go n.monitorConnection(ctx)

			return nil
		}
	}
}

func (n *GCli[T]) monitorConnection(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			state := n.conn.GetState()
			if state == connectivity.TransientFailure || state == connectivity.Shutdown {
				n.logger.Errorf(ctx, "соединение с сервисом %s потеряно. Попытка переподключения", n.host)
				if err := n.Init(ctx); err != nil {
					n.logger.Errorf(ctx, "не удалось переподключиться к сервису %s: %v", n.host, err)
				}
				return
			}
		}
	}
}

func (n *GCli[T]) Dispose() error {
	if n.conn != nil {
		return n.conn.Close()
	}
	return nil
}

func min(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}
