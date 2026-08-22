// Drives a running seedstone with go-redis.
//
// The client is deliberately built with no Protocol option. Version 9 of this
// library opens every connection by sending HELLO 3, so leaving the default
// alone means the mere act of connecting exercises the RESP3 refusal and the
// fallback to RESP2 — the one path in this gate that no other client takes,
// and the reason the refusal's text is a contract rather than a message.
package e2e

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func addr() string {
	if a := os.Getenv("SEEDSTONE_ADDR"); a != "" {
		return a
	}
	return "127.0.0.1:6390"
}

func client(t *testing.T) (*redis.Client, context.Context) {
	t.Helper()
	// The lane's password, or the empty string when the server was started
	// open — go-redis sends no AUTH for an empty one. A literal in CI that
	// protects nothing: it exists so this lane exercises the authenticated
	// path rather than the open one.
	c := redis.NewClient(&redis.Options{Addr: addr(), Password: os.Getenv("SEEDSTONE_PASSWORD")})
	t.Cleanup(func() { _ = c.Close() })
	ctx := context.Background()
	// These tests own these keys; a server left running from an earlier run
	// would otherwise decide the second run's counter.
	if err := c.Del(ctx, "gk", "gk2", "gn", "gfresh", "gbrief").Err(); err != nil {
		t.Fatalf("clearing this test's keys: %v", err)
	}
	return c, ctx
}

func TestRoundtrip(t *testing.T) {
	c, ctx := client(t)

	if got, err := c.Ping(ctx).Result(); err != nil || got != "PONG" {
		t.Fatalf("ping: %q, %v", got, err)
	}
	if err := c.Set(ctx, "gk", "v", 0).Err(); err != nil {
		t.Fatalf("set: %v", err)
	}
	if got, err := c.Get(ctx, "gk").Result(); err != nil || got != "v" {
		t.Fatalf("get: %q, %v", got, err)
	}
	if err := c.Set(ctx, "gk2", "v2", 100*time.Second).Err(); err != nil {
		t.Fatalf("set with expiry: %v", err)
	}
	if got, err := c.TTL(ctx, "gk2").Result(); err != nil || got <= 90*time.Second || got > 100*time.Second {
		t.Fatalf("ttl: %v, %v", got, err)
	}
	if got, err := c.Exists(ctx, "gk", "gk2", "gmissing").Result(); err != nil || got != 2 {
		t.Fatalf("exists: %d, %v", got, err)
	}
	if got, err := c.Del(ctx, "gk", "gk2").Result(); err != nil || got != 2 {
		t.Fatalf("del: %d, %v", got, err)
	}
	if got, err := c.IncrBy(ctx, "gn", 5).Result(); err != nil || got != 5 {
		t.Fatalf("incrby: %d, %v", got, err)
	}
	// SetArgs, not the library's SetNX helper: that one sends the standalone
	// SETNX command, which this server does not have. SetArgs is how go-redis
	// spells `SET key value NX`, and the refusal comes back as redis.Nil.
	nx := redis.SetArgs{Mode: "NX"}
	if got, err := c.SetArgs(ctx, "gfresh", "a", nx).Result(); err != nil || got != "OK" {
		t.Fatalf("set nx on a fresh key: %q, %v", got, err)
	}
	if _, err := c.SetArgs(ctx, "gfresh", "b", nx).Result(); err != redis.Nil {
		t.Fatalf("set nx on a taken key: %v", err)
	}
	if got, err := c.Get(ctx, "gfresh").Result(); err != nil || got != "a" {
		t.Fatalf("the value the refusal left alone: %q, %v", got, err)
	}
	// A missing key is redis.Nil, not an error the caller has to guess at.
	if _, err := c.Get(ctx, "gmissing").Result(); err != redis.Nil {
		t.Fatalf("get of a missing key: %v", err)
	}
}

// The handshake itself is the subject: this client sent HELLO 3, was refused,
// and is talking RESP2 by the time anything below runs.
func TestRESP2Fallback(t *testing.T) {
	c, ctx := client(t)

	if err := c.Do(ctx, "HELLO", 3).Err(); err == nil {
		t.Fatal("HELLO 3 was accepted; the fallback this client depends on is gone")
	} else if err.Error() != "NOPROTO unsupported protocol version" {
		t.Fatalf("HELLO 3 refusal text: %q", err.Error())
	}
	// And the connection that was refused is still usable, which is what makes
	// it a fallback rather than a disconnect.
	if got, err := c.Ping(ctx).Result(); err != nil || got != "PONG" {
		t.Fatalf("ping after the refusal: %q, %v", got, err)
	}
}

func TestExpirationIsObserved(t *testing.T) {
	c, ctx := client(t)

	if err := c.Set(ctx, "gbrief", "x", 200*time.Millisecond).Err(); err != nil {
		t.Fatalf("set with a short deadline: %v", err)
	}
	time.Sleep(500 * time.Millisecond)
	if _, err := c.Get(ctx, "gbrief").Result(); err != redis.Nil {
		t.Fatalf("a key past its deadline: %v", err)
	}
	if got, err := c.TTL(ctx, "gbrief").Result(); err != nil || got != -2*time.Nanosecond {
		t.Fatalf("ttl of an expired key: %v, %v", got, err)
	}
}
