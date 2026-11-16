package grpcutil

import (
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

const (
	// DefaultKeepaliveTime is the interval between HTTP/2 keepalive pings (30s per architecture spec).
	DefaultKeepaliveTime = 30 * time.Second
	// DefaultKeepaliveTimeout is the wait duration for a keepalive ping ack.
	DefaultKeepaliveTimeout = 10 * time.Second
	// DefaultPoolSize is the default number of pooled gRPC channels per target.
	DefaultPoolSize = 4
)

var (
	// ErrNoTargets is returned when no gRPC targets are configured.
	ErrNoTargets = errors.New("grpcutil: at least one target or connection is required")
	// ErrPoolClosed is returned when acquiring a connection from a closed pool.
	ErrPoolClosed = errors.New("grpcutil: connection pool is closed")
)

// Config configures a pooled set of multiplexed HTTP/2 gRPC channels.
type Config struct {
	Targets             []string
	PoolSize            int
	KeepaliveTime       time.Duration
	KeepaliveTimeout    time.Duration
	PermitWithoutStream bool
	DialOptions         []grpc.DialOption
}

// ConnPool distributes RPCs across multiple gRPC ClientConn channels using lock-free round-robin
// to prevent HTTP/2 single-connection saturation at high RPS.
type ConnPool struct {
	conns  []*grpc.ClientConn
	next   atomic.Uint64
	closed atomic.Bool
}

// NewConnPool dials PoolSize gRPC channels per target with 30s TCP/HTTP2 keepalives.
func NewConnPool(cfg Config) (*ConnPool, error) {
	if len(cfg.Targets) == 0 {
		return nil, ErrNoTargets
	}
	poolSize := cfg.PoolSize
	if poolSize <= 0 {
		poolSize = DefaultPoolSize
	}
	kaTime := cfg.KeepaliveTime
	if kaTime <= 0 {
		kaTime = DefaultKeepaliveTime
	}
	kaTimeout := cfg.KeepaliveTimeout
	if kaTimeout <= 0 {
		kaTimeout = DefaultKeepaliveTimeout
	}

	opts := make([]grpc.DialOption, 0, len(cfg.DialOptions)+1)
	opts = append(opts, grpc.WithKeepaliveParams(keepalive.ClientParameters{
		Time:                kaTime,
		Timeout:             kaTimeout,
		PermitWithoutStream: cfg.PermitWithoutStream,
	}))
	opts = append(opts, cfg.DialOptions...)

	conns := make([]*grpc.ClientConn, 0, len(cfg.Targets)*poolSize)
	for _, target := range cfg.Targets {
		for i := 0; i < poolSize; i++ {
			conn, err := grpc.NewClient(target, opts...)
			if err != nil {
				for _, c := range conns {
					_ = c.Close()
				}
				return nil, fmt.Errorf("grpcutil: dial %s: %w", target, err)
			}
			conns = append(conns, conn)
		}
	}

	return &ConnPool{conns: conns}, nil
}

// NewConnPoolFromConns wraps pre-existing gRPC connections (e.g. bufconn test connections).
func NewConnPoolFromConns(conns []*grpc.ClientConn) (*ConnPool, error) {
	if len(conns) == 0 {
		return nil, ErrNoTargets
	}
	copied := make([]*grpc.ClientConn, len(conns))
	copy(copied, conns)
	return &ConnPool{conns: copied}, nil
}

// Get returns the next gRPC ClientConn in round-robin order.
func (p *ConnPool) Get() (*grpc.ClientConn, error) {
	if p == nil || p.closed.Load() || len(p.conns) == 0 {
		return nil, ErrPoolClosed
	}
	idx := (p.next.Add(1) - 1) % uint64(len(p.conns))
	return p.conns[idx], nil
}

// Len returns the number of pooled connections.
func (p *ConnPool) Len() int {
	if p == nil {
		return 0
	}
	return len(p.conns)
}

// Close closes all pooled gRPC connections.
func (p *ConnPool) Close() error {
	if p == nil || !p.closed.CompareAndSwap(false, true) {
		return nil
	}
	var errs []error
	for _, conn := range p.conns {
		if conn != nil {
			if err := conn.Close(); err != nil {
				errs = append(errs, err)
			}
		}
	}
	return errors.Join(errs...)
}
